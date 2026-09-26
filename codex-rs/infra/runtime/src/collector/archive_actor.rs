use std::io;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::CollectorArchiveWorker;
use crate::ArchiveController;
use crate::ArchiveWorkerState;

/// Discovers finished collectors and replays durable archive preparation. Stop
/// waits for the active step and hands back the queue; it does not imply that
/// every collector or remote archive job has finished.
pub struct CollectorArchiveActor {
    stop: oneshot::Sender<()>,
    state: watch::Receiver<ArchiveWorkerState>,
    task: JoinHandle<io::Result<CollectorArchiveWorker>>,
}

impl CollectorArchiveActor {
    pub fn start(
        mut worker: CollectorArchiveWorker,
        controller: ArchiveController,
        interval: Duration,
    ) -> io::Result<Self> {
        if interval.is_zero() {
            return Err(io::Error::other(
                "collector archive interval must be positive",
            ));
        }
        let (stop, mut stopped) = oneshot::channel();
        let (status, state) = watch::channel(ArchiveWorkerState::Idle);
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = timer.tick() => {
                        let (returned, discovery) = on_worker(worker, CollectorArchiveWorker::discover_one).await?;
                        worker = returned;
                        // Discovery failures do not prevent already prepared
                        // attachments from advancing toward remote receipts.
                        let (returned, pending) = on_worker(worker, CollectorArchiveWorker::next_pending).await?;
                        worker = returned;
                        let result = match pending {
                            Ok(Some((key, jobs))) => {
                                let archived = async {
                                    jobs.submit(&controller).await?;
                                    jobs.completion(&controller).await
                                }.await;
                                match archived {
                                    Ok(Some(_)) => {
                                        let (returned, completed) = on_worker(worker, move |worker| worker.complete(&key)).await?;
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
                        status.send_replace(match (discovery, result) {
                            (Ok(()), Ok(state)) => state,
                            (Err(error), Ok(_)) | (Ok(()), Err(error)) => ArchiveWorkerState::RetryPending(error.to_string()),
                            (Err(discovery), Err(archive)) => ArchiveWorkerState::RetryPending(format!("discovery: {discovery}; archive: {archive}")),
                        });
                    }
                }
            }
            status.send_replace(ArchiveWorkerState::Stopped);
            Ok(worker)
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

    pub async fn stop(self) -> io::Result<CollectorArchiveWorker> {
        let _ = self.stop.send(());
        self.task.await.map_err(io::Error::other)?
    }
}

async fn on_worker<T: Send + 'static>(
    mut worker: CollectorArchiveWorker,
    action: impl FnOnce(&mut CollectorArchiveWorker) -> T + Send + 'static,
) -> io::Result<(CollectorArchiveWorker, T)> {
    tokio::task::spawn_blocking(move || {
        let result = action(&mut worker);
        (worker, result)
    })
    .await
    .map_err(io::Error::other)
}
