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
use crate::SessionController;
use crate::TransportArchiveJobIds;

mod journal;
use journal::ArchiveJournal;

/// Periodic transport snapshots, with one durable pending batch. The Session
/// actor only samples positions; disk persistence and remote receipt waits run
/// here, independently of the transport's input and forwarding loop.
pub struct TransportArchiveActor {
    stop: oneshot::Sender<()>,
    state: watch::Receiver<ArchiveWorkerState>,
    task: JoinHandle<io::Result<()>>,
}

impl TransportArchiveActor {
    pub async fn start(
        session: SessionController,
        archive: ArchiveController,
        directory: PathBuf,
        interval: Duration,
    ) -> io::Result<Self> {
        if interval.is_zero() {
            return Err(io::Error::other(
                "transport archive interval must be positive",
            ));
        }
        let (mut journal, receipts) = tokio::task::spawn_blocking(move || {
            let receipts = directory.join("receipts");
            fs::create_dir_all(&receipts)?;
            Ok::<_, io::Error>((
                ArchiveJournal::open(&directory.join("jobs.journal"))?,
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
                        let sampled = session.archive_snapshot(receipts.clone(), TransportArchiveJobIds {
                            directory: MessageId::new(), readiness: MessageId::new(), network: MessageId::new(),
                            network_cursor: MessageId::new(), inbox: MessageId::new(), observations: MessageId::new(), binding: MessageId::new(),
                        }).await;
                        let sample_error = sampled.as_ref().err().map(ToString::to_string);
                        let (returned, prepared) = on_journal(journal, move |journal| {
                            match sampled {
                                Ok(jobs) => journal.prepare(jobs).map(Some),
                                Err(_) => Ok(journal.current.clone()),
                            }
                        }).await?;
                        journal = returned;
                        let result = match prepared {
                            Ok(Some(jobs)) => {
                                let result = async {
                                    jobs.submit(&archive).await?;
                                    jobs.completion(&archive).await
                                }.await;
                                match result {
                                    Ok(Some(receipts)) => {
                                        let (returned, completed) = on_journal(journal, move |journal| journal.complete(receipts)).await?;
                                        journal = returned;
                                        completed.map(|()| ArchiveWorkerState::Idle)
                                    }
                                    Ok(None) => Ok(ArchiveWorkerState::Advancing),
                                    Err(error) => Err(error),
                                }
                            }
                            Ok(None) => Ok(ArchiveWorkerState::Idle),
                            Err(error) => Err(error),
                        };
                        status.send_replace(match (sample_error, result) {
                            (None, Ok(state)) => state,
                            (Some(error), Ok(_)) => ArchiveWorkerState::RetryPending(error),
                            (None, Err(error)) => ArchiveWorkerState::RetryPending(error.to_string()),
                            (Some(sample), Err(archive)) => ArchiveWorkerState::RetryPending(format!("snapshot: {sample}; archive: {archive}")),
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

    /// Waits for the active step. Pending jobs remain in the journal for restart;
    /// stopping the pump does not declare the transport streams complete.
    pub async fn stop(self) -> io::Result<()> {
        let _ = self.stop.send(());
        self.task.await.map_err(io::Error::other)?
    }
}

async fn on_journal<T: Send + 'static>(
    mut journal: ArchiveJournal,
    action: impl FnOnce(&mut ArchiveJournal) -> T + Send + 'static,
) -> io::Result<(ArchiveJournal, T)> {
    tokio::task::spawn_blocking(move || {
        let result = action(&mut journal);
        (journal, result)
    })
    .await
    .map_err(io::Error::other)
}
