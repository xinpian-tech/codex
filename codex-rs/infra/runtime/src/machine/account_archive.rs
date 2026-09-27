use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use codex_infra_account::AccountDirectory;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;
use codex_infra_state::Journal;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::ArchiveController;
use crate::ArchiveJob;
use crate::ArchiveTarget;
use crate::ArchiveWorkerState;

pub(super) struct AccountArchiveActor {
    stop: oneshot::Sender<()>,
    task: JoinHandle<io::Result<()>>,
    state: watch::Receiver<ArchiveWorkerState>,
}

struct Jobs {
    journal: Journal,
    template: ArchiveJob,
    current: Option<ArchiveJob>,
}

enum Sample {
    Periodic,
    Shutdown,
}

impl AccountArchiveActor {
    pub(super) async fn start(
        source: AccountDirectory,
        archive: ArchiveController,
        root_session_id: RootSessionId,
        machine_id: MachineId,
        directory: PathBuf,
        interval: Duration,
    ) -> io::Result<Self> {
        if interval.is_zero() {
            return Err(io::Error::other(
                "account archive interval must be positive",
            ));
        }
        let opening_source = source.clone();
        let mut jobs = tokio::task::spawn_blocking(move || {
            fs::create_dir_all(&directory)?;
            let directory = directory.canonicalize()?;
            let (source, position) = opening_source.archive_snapshot()?;
            let receipt_journal = directory.join("receipts.journal");
            let mut current: Option<ArchiveJob> = None;
            let journal = Journal::open(&directory.join("jobs.journal"), |record| {
                let job: ArchiveJob = serde_json::from_slice(&record.payload)?;
                if job.source != source
                    || job.receipt_journal != receipt_journal
                    || job.stream.root_session_id != root_session_id
                    || job.stream.machine_id != machine_id
                    || job.stream.name != "account-directory"
                    || !matches!(job.stream.producer, ArchiveProducer::Machine { .. })
                    || !matches!(job.target, ArchiveTarget::Snapshot(_))
                {
                    return Err(io::Error::other("account archive binding differs"));
                }
                if let Some(previous) = &current {
                    check_successor(previous, &job)?;
                }
                current = Some(job);
                Ok(())
            })?;
            let template = current.clone().unwrap_or_else(|| ArchiveJob {
                job_id: MessageId::new(),
                source,
                receipt_journal,
                stream: ArchiveStream {
                    root_session_id,
                    machine_id,
                    producer: ArchiveProducer::Machine {
                        machine_run_id: MessageId::new(),
                    },
                    name: "account-directory".to_owned(),
                },
                target: ArchiveTarget::Snapshot(position),
            });
            Ok::<_, io::Error>(Jobs {
                journal,
                template,
                current,
            })
        })
        .await
        .map_err(io::Error::other)??;
        let (stop, mut stopped) = oneshot::channel();
        let (status, state) = watch::channel(ArchiveWorkerState::Idle);
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                let sample = tokio::select! {
                    biased;
                    _ = &mut stopped => Sample::Shutdown,
                    _ = timer.tick() => Sample::Periodic,
                };
                let shutdown = matches!(sample, Sample::Shutdown);
                let (returned, result) = advance(jobs, source.clone(), &archive, sample).await?;
                jobs = returned;
                if shutdown {
                    status.send_replace(ArchiveWorkerState::Stopped);
                    return result.map(|_| ());
                }
                status.send_replace(match result {
                    Ok(state) => state,
                    Err(error) => ArchiveWorkerState::RetryPending(error.to_string()),
                });
            }
        });
        Ok(Self { stop, task, state })
    }

    pub(super) fn state(&self) -> ArchiveWorkerState {
        if self.state.has_changed().is_err() {
            ArchiveWorkerState::Stopped
        } else {
            self.state.borrow().clone()
        }
    }

    /// Queues the latest prefix before relinquishing the writer. This is a
    /// snapshot admission boundary, not remote completion or producer sealing.
    pub(super) async fn stop(self) -> io::Result<()> {
        let _ = self.stop.send(());
        self.task.await.map_err(io::Error::other)?
    }
}

async fn advance(
    jobs: Jobs,
    source: AccountDirectory,
    archive: &ArchiveController,
    sample: Sample,
) -> io::Result<(Jobs, io::Result<ArchiveWorkerState>)> {
    if let Some(job) = &jobs.current {
        let admitted = async {
            archive.enqueue(job.clone()).await?;
            match sample {
                Sample::Periodic => archive
                    .completion(job.job_id)
                    .await
                    .map(|receipt| receipt.is_some()),
                Sample::Shutdown => Ok(true),
            }
        }
        .await;
        match admitted {
            Ok(true) => {}
            Ok(false) => return Ok((jobs, Ok(ArchiveWorkerState::Advancing))),
            Err(error) => return Ok((jobs, Err(error))),
        }
    }
    let (jobs, prepared) = tokio::task::spawn_blocking(move || {
        let mut jobs = jobs;
        let result = (|| {
            let (path, position) = source.archive_snapshot()?;
            if path != jobs.template.source {
                return Err(io::Error::other("account archive source changed"));
            }
            let mut next = jobs.template.clone();
            next.job_id = MessageId::new();
            next.target = ArchiveTarget::Snapshot(position);
            if let Some(previous) = &jobs.current {
                check_successor(previous, &next)?;
                if previous.target == next.target {
                    return Ok(None);
                }
            }
            jobs.journal.append(&serde_json::to_vec(&next)?)?;
            jobs.current = Some(next.clone());
            Ok(Some(next))
        })();
        (jobs, result)
    })
    .await
    .map_err(io::Error::other)?;
    let result = match prepared {
        Ok(Some(job)) => archive
            .enqueue(job)
            .await
            .map(|_| ArchiveWorkerState::Advancing),
        Ok(None) => Ok(ArchiveWorkerState::Idle),
        Err(error) => Err(error),
    };
    Ok((jobs, result))
}

fn check_successor(previous: &ArchiveJob, next: &ArchiveJob) -> io::Result<()> {
    if previous.stream != next.stream {
        return Err(io::Error::other("account archive stream changed"));
    }
    match (&previous.target, &next.target) {
        (ArchiveTarget::Snapshot(previous), ArchiveTarget::Snapshot(next))
            if next.byte_offset >= previous.byte_offset
                && next.next_sequence >= previous.next_sequence =>
        {
            Ok(())
        }
        (ArchiveTarget::Snapshot(_), ArchiveTarget::Snapshot(_))
        | (ArchiveTarget::ProducerFinished(_), _)
        | (ArchiveTarget::Snapshot(_), ArchiveTarget::ProducerFinished(_)) => Err(
            io::Error::other("account archive position regressed or sealed"),
        ),
    }
}
