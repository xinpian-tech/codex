use std::io;
use std::time::Duration;

use codex_infra_protocol::MessageId;
use codex_infra_state::ArchiveReceipt;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::CollectorArchiveJobs;
use super::CollectorArchiveWorker;
use crate::ArchiveController;
use crate::ArchiveWorkerState;

/// Exact final jobs and remote receipts for one attachment. The machine finalizer
/// persists this result before reporting that attachment's archive complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorArchiveCompletion {
    pub attachment_id: MessageId,
    pub jobs: CollectorArchiveJobs,
    pub receipts: Vec<ArchiveReceipt>,
}

struct CompletionQuery {
    attachment_id: MessageId,
    reply: oneshot::Sender<io::Result<Option<CollectorArchiveCompletion>>>,
}

/// Discovers collector prefixes and endings, replaying durable preparation. Stop
/// waits for the active step and hands back the queue; it does not imply that
/// every collector or remote archive job has finished.
pub struct CollectorArchiveActor {
    stop: oneshot::Sender<()>,
    queries: mpsc::Sender<CompletionQuery>,
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
        let (queries, mut requests) = mpsc::channel::<CompletionQuery>(16);
        let (status, state) = watch::channel(ArchiveWorkerState::Idle);
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    query = requests.recv() => {
                        let Some(query) = query else { break; };
                        let attachment_id = query.attachment_id;
                        let (returned, jobs) = on_worker(worker, move |worker| worker.finished_jobs(attachment_id)).await?;
                        worker = returned;
                        let result = match jobs {
                            Ok(Some(jobs)) => match jobs.completion(&controller).await {
                                Ok(Some(receipts)) => {
                                    let (returned, completed) = on_worker(worker, move |worker| worker.complete(&attachment_id.to_string())).await?;
                                    worker = returned;
                                    completed.map(|()| Some(CollectorArchiveCompletion { attachment_id, jobs, receipts }))
                                }
                                Ok(None) => Ok(None),
                                Err(error) => Err(error),
                            },
                            Ok(None) => Ok(None),
                            Err(error) => Err(error),
                        };
                        let _ = query.reply.send(result);
                    }
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
        Ok(Self {
            stop,
            queries,
            state,
            task,
        })
    }

    pub fn state(&self) -> ArchiveWorkerState {
        if self.state.has_changed().is_err() {
            ArchiveWorkerState::Stopped
        } else {
            self.state.borrow().clone()
        }
    }

    /// None means the final batch is not yet prepared or archived. Live snapshot
    /// receipts do not count. Cancellation drops only this query's waiter; the
    /// archive actor retains ownership of every previously prepared batch.
    pub async fn completion(
        &self,
        attachment_id: MessageId,
    ) -> io::Result<Option<CollectorArchiveCompletion>> {
        let (reply, result) = oneshot::channel();
        self.queries
            .send(CompletionQuery {
                attachment_id,
                reply,
            })
            .await
            .map_err(|_| io::Error::other("collector archive actor stopped"))?;
        result
            .await
            .map_err(|_| io::Error::other("collector archive completion reply lost"))?
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
