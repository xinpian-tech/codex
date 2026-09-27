use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use codex_infra_account::AccountExchangeObserver;
use codex_infra_account::recover_account_exchange;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;
use codex_infra_state::Journal;
use codex_infra_state::QueueItem;
use codex_infra_state::SpoolQueue;
use serde_json::json;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::ArchiveController;
use crate::ArchiveJob;
use crate::ArchiveTarget;
use crate::ArchiveWorkerState;

pub(super) struct ExchangeArchiveConfig {
    pub root_session_id: RootSessionId,
    pub machine_id: MachineId,
    pub exchanges: PathBuf,
    pub directory: PathBuf,
    pub interval: Duration,
}

struct Worker {
    config: ExchangeArchiveConfig,
    _binding: Journal,
    entries: fs::ReadDir,
    receipts: PathBuf,
    queue: SpoolQueue,
    last_lane: Option<String>,
}

impl Worker {
    fn open(mut config: ExchangeArchiveConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.exchanges)?;
        fs::create_dir_all(&config.directory)?;
        config.exchanges = config.exchanges.canonicalize()?;
        config.directory = config.directory.canonicalize()?;
        let expected = json!({"root_session_id": config.root_session_id, "machine_id": config.machine_id, "exchanges": config.exchanges});
        let mut opened = false;
        let mut binding = Journal::open(&config.directory.join("binding.journal"), |record| {
            if opened || serde_json::from_slice::<serde_json::Value>(&record.payload)? != expected {
                return Err(io::Error::other("account exchange archive binding changed"));
            }
            opened = true;
            Ok(())
        })?;
        if !opened {
            binding.append(&serde_json::to_vec(&expected)?)?;
        }
        let receipts = config.directory.join("receipts");
        fs::create_dir_all(&receipts)?;
        Ok(Self {
            entries: fs::read_dir(&config.exchanges)?,
            queue: SpoolQueue::open(&config.directory.join("jobs.journal"))?,
            config,
            _binding: binding,
            receipts,
            last_lane: None,
        })
    }

    fn discover(&mut self) -> io::Result<()> {
        let Some(entry) = self.entries.next() else {
            self.entries = fs::read_dir(&self.config.exchanges)?;
            return Ok(());
        };
        self.prepare(entry?)
    }

    fn prepare(&mut self, entry: fs::DirEntry) -> io::Result<()> {
        if !entry.file_type()?.is_dir() {
            return Ok(());
        }
        let Some(finished) = recover_account_exchange(&entry.path())? else {
            return Ok(());
        };
        let identity = finished.identity;
        let key = identity.connection_id.to_string();
        let (machine_id, producer) = match identity.observer {
            AccountExchangeObserver::Service => (
                identity.authority.machine_id,
                ArchiveProducer::Machine {
                    machine_run_id: identity.authority.instance_id,
                },
            ),
            AccountExchangeObserver::Agent {
                machine_id,
                agent_id,
                launch_id,
            } => (
                machine_id,
                ArchiveProducer::Agent {
                    agent_id,
                    launch_id,
                },
            ),
        };
        if identity.root_session_id != self.config.root_session_id
            || machine_id != self.config.machine_id
            || entry.file_name() != std::ffi::OsStr::new(&key)
        {
            return Err(io::Error::other(
                "account exchange completion identity differs",
            ));
        }
        let job = ArchiveJob {
            job_id: identity.connection_id,
            stream: ArchiveStream {
                root_session_id: identity.root_session_id,
                machine_id,
                producer,
                name: format!("account-{key}-io"),
            },
            source: entry.path().join("io.journal").canonicalize()?,
            receipt_journal: self.receipts.join(format!("{key}.journal")),
            target: ArchiveTarget::ProducerFinished(finished.position),
        };
        self.queue.enqueue(QueueItem {
            key: key.clone(),
            lane: key,
            payload: serde_json::to_vec(&job)?,
        })?;
        Ok(())
    }

    fn pending(&mut self) -> io::Result<Option<(String, ArchiveJob)>> {
        let lane = self
            .queue
            .lanes()
            .find(|lane| self.last_lane.as_deref().is_none_or(|last| *lane > last))
            .or_else(|| self.queue.lanes().next())
            .map(str::to_owned);
        let Some(lane) = lane else { return Ok(None) };
        self.last_lane = Some(lane.clone());
        let keys = self
            .queue
            .pending_keys(&lane, /*first_sequence*/ 0, NonZeroUsize::MIN);
        let key = keys
            .first()
            .ok_or_else(|| io::Error::other("account archive lane is empty"))?;
        let item = self.queue.read(key)?;
        Ok(Some((item.key, serde_json::from_slice(&item.payload)?)))
    }

    fn final_scan(&mut self) -> io::Result<()> {
        let mut failure = None;
        for entry in fs::read_dir(&self.config.exchanges)? {
            if let Err(error) = entry.and_then(|entry| self.prepare(entry)) {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

/// Discovers one completed connection and advances one pending job per tick.
/// Completion remains tied to the machine writer's durable remote receipt.
pub(super) struct ExchangeArchiveActor {
    stop: oneshot::Sender<()>,
    task: JoinHandle<io::Result<()>>,
    state: watch::Receiver<ArchiveWorkerState>,
}

impl ExchangeArchiveActor {
    pub(super) async fn start(
        config: ExchangeArchiveConfig,
        archive: ArchiveController,
    ) -> io::Result<Self> {
        let interval = config.interval;
        if interval.is_zero() {
            return Err(io::Error::other(
                "account exchange archive interval must be positive",
            ));
        }
        let mut worker = tokio::task::spawn_blocking(move || Worker::open(config))
            .await
            .map_err(io::Error::other)??;
        let (stop, mut stopped) = oneshot::channel();
        let (status, state) = watch::channel(ArchiveWorkerState::Idle);
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    _ = timer.tick() => {}
                }
                let (returned, discovered) = on_worker(worker, Worker::discover).await?;
                worker = returned;
                let (returned, pending) = on_worker(worker, Worker::pending).await?;
                worker = returned;
                let advanced = match pending {
                    Ok(Some((key, job))) => {
                        let result = async {
                            let id = job.job_id;
                            archive.enqueue(job).await?;
                            archive.completion(id).await
                        }
                        .await;
                        match result {
                            Ok(Some(_)) => {
                                let (returned, completed) =
                                    on_worker(worker, move |worker| worker.queue.complete(&key))
                                        .await?;
                                worker = returned;
                                completed.map(|()| ArchiveWorkerState::Advancing)
                            }
                            Ok(None) => Ok(ArchiveWorkerState::Advancing),
                            Err(error) => Err(error),
                        }
                    }
                    Ok(None) => Ok(ArchiveWorkerState::Idle),
                    Err(error) => Err(error),
                };
                status.send_replace(match (discovered, advanced) {
                    (Ok(()), Ok(state)) => state,
                    (Err(error), Ok(_)) | (Ok(()), Err(error)) => {
                        ArchiveWorkerState::RetryPending(error.to_string())
                    }
                    (Err(discovery), Err(archive)) => ArchiveWorkerState::RetryPending(format!(
                        "discovery: {discovery}; archive: {archive}"
                    )),
                });
            }
            // Account services have drained before an explicit stop. Discover
            // their final markers and transfer each remaining job once.
            let (returned, scanned) = on_worker(worker, Worker::final_scan).await?;
            worker = returned;
            let mut failure = scanned.err();
            let mut after = None::<String>;
            loop {
                let (returned, next) = on_worker(worker, move |worker| {
                    let lane = worker
                        .queue
                        .lanes()
                        .find(|lane| after.as_deref().is_none_or(|after| *lane > after))
                        .map(str::to_owned);
                    let Some(lane) = lane else {
                        return Ok::<_, io::Error>(None);
                    };
                    let item = worker.queue.read(&lane)?;
                    Ok(Some((
                        lane,
                        serde_json::from_slice::<ArchiveJob>(&item.payload)?,
                    )))
                })
                .await?;
                worker = returned;
                let Some((lane, job)) = next? else { break };
                after = Some(lane);
                if let Err(error) = archive.enqueue(job).await {
                    failure.get_or_insert(error);
                }
            }
            status.send_replace(ArchiveWorkerState::Stopped);
            failure.map_or(Ok(()), Err)
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

    /// After services drain, scans their final markers and durably admits every
    /// pending job to the machine writer. Remote receipts may still be pending.
    pub(super) async fn stop(self) -> io::Result<()> {
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
