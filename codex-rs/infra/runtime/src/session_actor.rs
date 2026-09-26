use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::DirectoryEvent;
use codex_infra_protocol::MessageId;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::TransportArchiveJobIds;
use crate::TransportArchiveJobs;
use crate::TransportSession;

/// Control updates contain bindings and directory metadata. Semantic messages
/// continue to arrive through the Agent pane transport.
pub enum SessionUpdate {
    Directory(Box<DirectoryEvent>),
    Launch {
        agent_id: AgentId,
        launch_id: MessageId,
    },
    AgentExited {
        agent_id: AgentId,
    },
}

enum Command {
    ArchiveSnapshot {
        receipts: PathBuf,
        ids: TransportArchiveJobIds,
        reply: oneshot::Sender<io::Result<TransportArchiveJobs>>,
    },
    Update {
        update: SessionUpdate,
        reply: oneshot::Sender<io::Result<()>>,
    },
    Stop,
}

#[derive(Clone)]
pub struct SessionController {
    commands: mpsc::Sender<Command>,
}

impl SessionController {
    /// Samples writer acknowledgments on the owning transport actor. Remote Git
    /// work is performed independently by the archive service after this reply.
    pub async fn archive_snapshot(
        &self,
        receipts: PathBuf,
        ids: TransportArchiveJobIds,
    ) -> io::Result<TransportArchiveJobs> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::ArchiveSnapshot {
                receipts,
                ids,
                reply,
            })
            .await
            .map_err(|_| io::Error::other("Session actor stopped"))?;
        result
            .await
            .map_err(|_| io::Error::other("Session archive snapshot reply lost"))?
    }

    pub async fn apply(&self, update: SessionUpdate) -> io::Result<()> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Update { update, reply })
            .await
            .map_err(|_| io::Error::other("Session actor stopped"))?;
        result
            .await
            .map_err(|_| io::Error::other("Session actor stopped before confirming update"))?
    }
}

/// Returning the Session preserves collector ownership and durable queues for
/// machine recovery/finalization. Stop is an actor handoff, not Agent completion.
pub struct SessionExit {
    pub session: TransportSession,
    pub failures: Vec<String>,
}

pub struct SessionActor {
    controller: SessionController,
    task: JoinHandle<SessionExit>,
}

impl SessionActor {
    pub fn start(
        mut session: TransportSession,
        interval: Duration,
        capacity: NonZeroUsize,
    ) -> io::Result<Self> {
        if interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Session interval must be positive",
            ));
        }
        let (commands, mut receiver) = mpsc::channel(capacity.get());
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut failures = Vec::new();
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        if let Err(error) = session.tick() {
                            failures.push(error.to_string());
                            break;
                        }
                    }
                    command = receiver.recv() => {
                        match command {
                            Some(Command::ArchiveSnapshot { receipts, ids, reply }) => {
                                let _ = reply.send(session.prepare_archive_snapshot(&receipts, ids));
                            }
                            Some(Command::Update { update, reply }) => {
                                let result = match update {
                                    SessionUpdate::Directory(event) => session.update_directory(*event).map(|_| ()),
                                    SessionUpdate::Launch { agent_id, launch_id } => session.register_launch(agent_id, launch_id),
                                    SessionUpdate::AgentExited { agent_id } => session.agent_exited(agent_id),
                                };
                                let _ = reply.send(result);
                            }
                            Some(Command::Stop) | None => break,
                        }
                    }
                }
            }
            receiver.close();
            while let Ok(command) = receiver.try_recv() {
                match command {
                    Command::ArchiveSnapshot { reply, .. } => {
                        let _ = reply.send(Err(io::Error::other(
                            "Session actor is handing off ownership",
                        )));
                    }
                    Command::Update { reply, .. } => {
                        let _ = reply.send(Err(io::Error::other(
                            "Session actor is handing off ownership",
                        )));
                    }
                    Command::Stop => {}
                }
            }
            if let Err(error) = session.drain_in_flight(interval).await {
                failures.push(error.to_string());
            }
            SessionExit { session, failures }
        });
        Ok(Self {
            controller: SessionController { commands },
            task,
        })
    }

    pub fn controller(&self) -> SessionController {
        self.controller.clone()
    }

    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub async fn stop(self) -> io::Result<SessionExit> {
        // A failed actor may already have closed the receiver; still recover its
        // owned Session and recorded failure through the join result.
        let _ = self.controller.commands.send(Command::Stop).await;
        self.task.await.map_err(io::Error::other)
    }
}
