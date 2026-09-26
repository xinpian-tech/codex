use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use codex_infra_protocol::MachineId;
use codex_infra_protocol::RootSessionId;
use codex_infra_provider::ProviderAttemptIdentity;
use codex_infra_provider::recover_provider_attempt;
use codex_infra_state::Journal;
use codex_infra_state::QueueItem;
use codex_infra_state::SpoolQueue;
use serde_json::json;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::ProviderArchiveJobs;
use crate::ArchiveController;
use crate::ArchiveWorkerState;

#[derive(Clone)]
pub struct ProviderArchiveConfig {
    pub root_session_id: RootSessionId,
    pub machine_id: MachineId,
    pub attempts: PathBuf,
    pub directory: PathBuf,
    pub interval: Duration,
}

struct Worker {
    config: ProviderArchiveConfig,
    _binding: Journal,
    entries: fs::ReadDir,
    receipts: PathBuf,
    queue: SpoolQueue,
    last_lane: Option<String>,
}

impl Worker {
    fn open(mut config: ProviderArchiveConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.attempts)?;
        fs::create_dir_all(&config.directory)?;
        config.attempts = config.attempts.canonicalize()?;
        config.directory = config.directory.canonicalize()?;
        let binding = json!({"root_session_id": config.root_session_id, "machine_id": config.machine_id, "attempts": config.attempts});
        let mut opened = false;
        let mut journal = Journal::open(&config.directory.join("binding.journal"), |record| {
            if opened || serde_json::from_slice::<serde_json::Value>(&record.payload)? != binding {
                return Err(io::Error::other("provider archive source binding changed"));
            }
            opened = true;
            Ok(())
        })?;
        if !opened {
            journal.append(&serde_json::to_vec(&binding)?)?;
        }
        let receipts = config.directory.join("receipts");
        fs::create_dir_all(&receipts)?;
        Ok(Self {
            entries: fs::read_dir(&config.attempts)?,
            queue: SpoolQueue::open(&config.directory.join("jobs.journal"))?,
            config,
            _binding: journal,
            receipts,
            last_lane: None,
        })
    }

    fn discover(&mut self) -> io::Result<()> {
        let Some(entry) = self.entries.next() else {
            self.entries = fs::read_dir(&self.config.attempts)?;
            return Ok(());
        };
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            return Ok(());
        }
        let Some(identity) = ProviderAttemptIdentity::read(&entry.path())? else {
            return Ok(());
        };
        if identity.root_session_id != self.config.root_session_id
            || identity.machine_id != self.config.machine_id
        {
            return Err(io::Error::other(
                "provider attempt belongs to another root/machine",
            ));
        }
        let key = identity.attempt_id.to_string();
        match self.queue.read(&key) {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if !self
            .queue
            .pending_keys(&key, 0, NonZeroUsize::MIN)
            .is_empty()
        {
            return Ok(());
        }
        let (job_key, jobs) = if recover_provider_attempt(&entry.path())?.is_some() {
            let Some(jobs) = ProviderArchiveJobs::prepare(&entry.path(), &self.receipts)? else {
                return Ok(());
            };
            (key.clone(), jobs)
        } else {
            let Some(jobs) = ProviderArchiveJobs::prepare_snapshot(&entry.path(), &self.receipts)?
            else {
                return Ok(());
            };
            let positions: Vec<_> = jobs
                .jobs
                .iter()
                .map(|job| (&job.stream, &job.target))
                .collect();
            let digest = blake3::hash(&serde_json::to_vec(&positions)?).to_hex();
            let job_key = format!("{key}/snapshot/{digest}");
            match self.queue.read(&job_key) {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            (job_key, jobs)
        };
        self.queue.enqueue(QueueItem {
            key: job_key,
            lane: key,
            payload: serde_json::to_vec(&jobs)?,
        })?;
        Ok(())
    }

    fn pending(&mut self) -> io::Result<Option<(String, ProviderArchiveJobs)>> {
        let lane = self
            .queue
            .lanes()
            .find(|lane| self.last_lane.as_deref().is_none_or(|last| *lane > last))
            .or_else(|| self.queue.lanes().next())
            .map(str::to_owned);
        let Some(lane) = lane else {
            return Ok(None);
        };
        self.last_lane = Some(lane.clone());
        let keys = self.queue.pending_keys(&lane, 0, NonZeroUsize::MIN);
        let key = keys
            .first()
            .ok_or_else(|| io::Error::other("provider archive lane empty"))?;
        let item = self.queue.read(key)?;
        Ok(Some((item.key, serde_json::from_slice(&item.payload)?)))
    }
}

/// Periodically discovers one attempt and advances one durable pending batch.
/// Preparation runs on blocking workers; remote work uses the machine writer.
pub struct ProviderArchiveActor {
    stop: oneshot::Sender<()>,
    state: watch::Receiver<ArchiveWorkerState>,
    task: JoinHandle<io::Result<()>>,
}

impl ProviderArchiveActor {
    pub async fn start(
        config: ProviderArchiveConfig,
        controller: ArchiveController,
    ) -> io::Result<Self> {
        let interval = config.interval;
        if interval.is_zero() {
            return Err(io::Error::other(
                "provider archive interval must be positive",
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
                    _ = &mut stopped => break,
                    _ = timer.tick() => {}
                }
                let (returned, discovered) = on_worker(worker, Worker::discover).await?;
                worker = returned;
                let (returned, pending) = on_worker(worker, Worker::pending).await?;
                worker = returned;
                let advanced = match pending {
                    Ok(Some((key, jobs))) => {
                        let result = async {
                            jobs.submit(&controller).await?;
                            jobs.completion(&controller).await
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

    /// Finishes the current step; pending batches remain in the durable queue.
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
