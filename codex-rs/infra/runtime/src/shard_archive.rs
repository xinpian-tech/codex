use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use codex_infra_protocol::MessageId;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::ArchiveController;
use crate::ArchiveWorkerState;
use crate::DispatchArchiveJobIds;
use crate::SessionController;

mod worker;
use worker::Candidate;
use worker::Worker;

/// Discovers per-attachment dispatch and per-Agent input streams. Preparation
/// runs on the owning Session; one durable pending batch per shard coalesces
/// snapshots while remote archival lags. Git work stays with ArchiveController.
pub struct ShardArchiveActor {
    stop: oneshot::Sender<()>,
    state: watch::Receiver<ArchiveWorkerState>,
    task: JoinHandle<io::Result<()>>,
}

impl ShardArchiveActor {
    pub async fn start(
        session: SessionController,
        archive: ArchiveController,
        session_directory: PathBuf,
        directory: PathBuf,
        interval: Duration,
    ) -> io::Result<Self> {
        if interval.is_zero() {
            return Err(io::Error::other("shard archive interval must be positive"));
        }
        let (mut worker, receipts) = tokio::task::spawn_blocking(move || {
            let receipts = directory.join("receipts");
            fs::create_dir_all(&receipts)?;
            Ok::<_, io::Error>((
                Worker::open(&session_directory, &directory)?,
                receipts.canonicalize()?,
            ))
        })
        .await
        .map_err(io::Error::other)??;
        let (stop, mut stopped) = oneshot::channel();
        let (status, state) = watch::channel(ArchiveWorkerState::Idle);
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = timer.tick() => {
                        let (returned, candidate) = on_worker(worker, Worker::discover).await?;
                        worker = returned;
                        let mut preparation_error = None;
                        match candidate {
                            Ok(Some(candidate)) => {
                                let prepared = match candidate {
                                    Candidate::Dispatch(id) => session.dispatch_archive(id, receipts.clone(), DispatchArchiveJobIds {
                                        queue: MessageId::new(), cursor: MessageId::new(), completion: MessageId::new(),
                                    }).await.map(|jobs| jobs.map(|jobs| jobs.jobs)),
                                    Candidate::Input(id) => session.input_archive(id, receipts.clone(), MessageId::new()).await.map(|job| job.map(|job| vec![job])),
                                };
                                match prepared {
                                    Ok(Some(jobs)) => {
                                        let (returned, staged) = on_worker(worker, move |worker| worker.stage(candidate, jobs)).await?;
                                        worker = returned;
                                        preparation_error = staged.err();
                                    }
                                    Ok(None) => {}
                                    Err(error) => preparation_error = Some(error),
                                }
                            }
                            Ok(None) => {}
                            Err(error) => preparation_error = Some(error),
                        }
                        let (returned, pending) = on_worker(worker, Worker::pending).await?;
                        worker = returned;
                        let outcome = match pending {
                            Ok(Some((key, jobs))) => {
                                let confirmed = async {
                                    for job in &jobs { archive.enqueue(job.clone()).await?; }
                                    for job in &jobs {
                                        if archive.completion(job.job_id).await?.is_none() { return Ok::<_, io::Error>(false); }
                                    }
                                    Ok(true)
                                }.await;
                                match confirmed {
                                    Ok(true) => {
                                        let (returned, completed) = on_worker(worker, move |worker| worker.complete(&key)).await?;
                                        worker = returned;
                                        completed.map(|()| ArchiveWorkerState::Advancing)
                                    }
                                    Ok(false) => Ok(ArchiveWorkerState::Advancing),
                                    Err(error) => Err(error),
                                }
                            }
                            Ok(None) => Ok(ArchiveWorkerState::Idle),
                            Err(error) => Err(error),
                        };
                        status.send_replace(match (preparation_error, outcome) {
                            (None, Ok(state)) => state,
                            (Some(error), Ok(_)) if error.kind() == io::ErrorKind::WouldBlock => ArchiveWorkerState::Advancing,
                            (Some(error), Ok(_)) | (None, Err(error)) => ArchiveWorkerState::RetryPending(error.to_string()),
                            (Some(prepare), Err(archive)) => ArchiveWorkerState::RetryPending(format!("prepare: {prepare}; archive: {archive}")),
                        });
                    }
                }
            }
            status.send_replace(ArchiveWorkerState::Stopped);
            Ok(())
        });
        Ok(Self { stop, state, task })
    }

    pub fn state(&self) -> ArchiveWorkerState {
        if self.state.has_changed().is_err() {
            ArchiveWorkerState::Stopped
        } else {
            self.state.borrow().clone()
        }
    }

    /// Waits for the current step, leaving unconfirmed batches durable for replay.
    pub async fn stop(self) -> io::Result<()> {
        let _ = self.stop.send(());
        self.task.await.map_err(io::Error::other)?
    }
}

async fn on_worker<T: Send + 'static>(
    mut worker: Worker,
    action: impl FnOnce(&mut Worker) -> T + Send + 'static,
) -> io::Result<(Worker, T)> {
    tokio::task::spawn_blocking(move || {
        let result = action(&mut worker);
        (worker, result)
    })
    .await
    .map_err(io::Error::other)
}
