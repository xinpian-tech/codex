use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;

use codex_infra_protocol::MessageId;
use codex_infra_state::ArchiveReceipt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::ArchiveJob;
use crate::MachineArchiveWriter;

enum Command {
    Enqueue {
        job: ArchiveJob,
        reply: oneshot::Sender<io::Result<u64>>,
    },
    Completion {
        job_id: MessageId,
        reply: oneshot::Sender<io::Result<Option<ArchiveReceipt>>>,
    },
    Stop,
}

/// The most recent scheduler observation, not a whole-machine health verdict.
#[derive(Clone, Debug)]
pub enum ArchiveWorkerState {
    Idle,
    Advancing,
    RetryPending(String),
    Stopped,
}

#[derive(Clone)]
pub struct ArchiveController {
    commands: mpsc::Sender<Command>,
    state: watch::Receiver<ArchiveWorkerState>,
}

impl ArchiveController {
    /// Success acknowledges durable admission. Cancelling the waiter does not
    /// cancel a command already being processed by the machine writer.
    pub async fn enqueue(&self, job: ArchiveJob) -> io::Result<u64> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Enqueue { job, reply })
            .await
            .map_err(|_| io::Error::other("archive actor stopped"))?;
        result
            .await
            .map_err(|_| io::Error::other("archive admission reply lost"))?
    }

    pub async fn completion(&self, job_id: MessageId) -> io::Result<Option<ArchiveReceipt>> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Completion { job_id, reply })
            .await
            .map_err(|_| io::Error::other("archive actor stopped"))?;
        result
            .await
            .map_err(|_| io::Error::other("archive completion reply lost"))?
    }

    pub fn state(&self) -> ArchiveWorkerState {
        if self.state.has_changed().is_err() {
            ArchiveWorkerState::Stopped
        } else {
            self.state.borrow().clone()
        }
    }
}

/// Stop hands the writer and any durable pending jobs back to its owner; it
/// does not claim those jobs or the Agent have completed. Each in-flight Git
/// operation is awaited before ownership is returned.
pub struct ArchiveActor {
    controller: ArchiveController,
    task: JoinHandle<io::Result<MachineArchiveWriter>>,
}

impl ArchiveActor {
    pub fn start(
        mut writer: MachineArchiveWriter,
        interval: Duration,
        capacity: NonZeroUsize,
    ) -> io::Result<Self> {
        if interval.is_zero() {
            return Err(io::Error::other("archive interval must be positive"));
        }
        let (commands, mut receiver) = mpsc::channel(capacity.get());
        let (status, state) = watch::channel(ArchiveWorkerState::Idle);
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        status.send_replace(ArchiveWorkerState::Advancing);
                        let (returned, result) = on_writer(writer, MachineArchiveWriter::advance_one).await?;
                        writer = returned;
                        status.send_replace(match result {
                            Ok(None) => ArchiveWorkerState::Idle,
                            Ok(Some(_)) => ArchiveWorkerState::Advancing,
                            Err(error) => ArchiveWorkerState::RetryPending(error.to_string()),
                        });
                    }
                    command = receiver.recv() => {
                        match command {
                            Some(Command::Enqueue { job, reply }) => {
                                let (returned, result) = on_writer(writer, move |writer| writer.enqueue(job)).await?;
                                writer = returned;
                                let _ = reply.send(result);
                            }
                            Some(Command::Completion { job_id, reply }) => {
                                let (returned, result) = on_writer(writer, move |writer| writer.completion(job_id)).await?;
                                writer = returned;
                                let _ = reply.send(result);
                            }
                            Some(Command::Stop) | None => break,
                        }
                    }
                }
            }
            receiver.close();
            // Dropping queued reply senders reports that admission/query never
            // completed. Already persisted jobs remain with the returned writer.
            drop(receiver);
            status.send_replace(ArchiveWorkerState::Stopped);
            Ok(writer)
        });
        Ok(Self {
            controller: ArchiveController { commands, state },
            task,
        })
    }

    pub fn controller(&self) -> ArchiveController {
        self.controller.clone()
    }

    pub async fn stop(self) -> io::Result<MachineArchiveWriter> {
        let _ = self.controller.commands.send(Command::Stop).await;
        self.task.await.map_err(io::Error::other)?
    }
}

async fn on_writer<T: Send + 'static>(
    mut writer: MachineArchiveWriter,
    action: impl FnOnce(&mut MachineArchiveWriter) -> T + Send + 'static,
) -> io::Result<(MachineArchiveWriter, T)> {
    tokio::task::spawn_blocking(move || {
        let result = action(&mut writer);
        (writer, result)
    })
    .await
    .map_err(io::Error::other)
}
