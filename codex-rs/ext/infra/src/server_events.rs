use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_app_server::in_process::InProcessClientSender;
use codex_app_server::in_process::InProcessServerEvent;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::StoreAuditIdentity;

mod consumer;
mod cursor;
mod reader;
mod reply;
pub use consumer::AgentEventConsumerExit;
pub use cursor::AgentEventCursor;
pub use reader::AgentServerEventPage;
pub use reader::AgentServerEventRecord;
pub use reply::AgentServerReplyOutcome;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentServerEvent {
    Opened {
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    },
    Request {
        request: Box<ServerRequest>,
    },
    Notification {
        notification: Box<ServerNotification>,
    },
    Lagged {
        skipped: usize,
    },
    ReplyPrepared {
        request_position: JournalPosition,
        response: Result<serde_json::Value, codex_app_server_protocol::JSONRPCErrorError>,
    },
    ReplySubmitted {
        request_position: JournalPosition,
        outcome: AgentServerReplyOutcome,
    },
    Closed,
}

pub(crate) struct PreparedServerEvents {
    path: PathBuf,
    journal: Journal,
    run_start: JournalPosition,
}

/// Independent event capture. Consumers follow the durable journal instead of
/// holding up the app-server receiver while they wait on model or tool RPCs.
/// A Lagged record preserves upstream loss explicitly; it is not reconstructed
/// as a complete notification history.
pub struct AgentServerEvents {
    path: PathBuf,
    run_start: JournalPosition,
    progress: watch::Receiver<Result<JournalPosition, String>>,
    task: tokio::task::JoinHandle<io::Result<JournalPosition>>,
    replies: mpsc::Sender<reply::ReplyCommand>,
}

impl PreparedServerEvents {
    pub(crate) fn open(
        path: &Path,
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    ) -> io::Result<Self> {
        let mut journal = Journal::open(path, |record| {
            if let AgentServerEvent::Opened {
                identity: previous,
                launch_id: previous_launch,
            } = serde_json::from_slice(&record.payload)?
                && (previous != identity || previous_launch != launch_id)
            {
                return Err(io::Error::other("server event launch binding changed"));
            }
            Ok(())
        })?;
        let run_start = journal.position();
        journal.append(&serde_json::to_vec(&AgentServerEvent::Opened {
            identity,
            launch_id,
        })?)?;
        Ok(Self {
            path: path.canonicalize()?,
            journal,
            run_start,
        })
    }

    pub(crate) fn start(
        self,
        mut receiver: mpsc::Receiver<InProcessServerEvent>,
        sender: InProcessClientSender,
    ) -> AgentServerEvents {
        let (replies, mut commands) = mpsc::channel::<reply::ReplyCommand>(receiver.max_capacity());
        let (progress, updates) = watch::channel(Ok(self.journal.position()));
        let path = self.path;
        let run_start = self.run_start;
        let task = tokio::spawn(async move {
            let result: io::Result<JournalPosition> = async {
                let mut journal = self.journal;
                let mut requests = BTreeMap::<u64, reply::LiveRequest>::new();
                loop {
                    let incoming = tokio::select! {
                        command = commands.recv(), if !commands.is_closed() || !commands.is_empty() => {
                            if let Some(command) = command {
                                journal = reply::handle(journal, &mut requests, &sender, command).await?;
                                let _ = progress.send_replace(Ok(journal.position()));
                            }
                            continue;
                        }
                        event = receiver.recv() => event,
                    };
                    let position = journal.position();
                    let event = match incoming {
                        Some(InProcessServerEvent::ServerRequest(request)) => {
                            requests.insert(position.next_sequence, reply::LiveRequest {
                                position, id: request.id().clone(), enqueued: None,
                            });
                            AgentServerEvent::Request { request }
                        }
                        Some(InProcessServerEvent::ServerNotification(notification)) => {
                            AgentServerEvent::Notification { notification }
                        }
                        Some(InProcessServerEvent::Lagged { skipped }) => {
                            AgentServerEvent::Lagged { skipped }
                        }
                        None => AgentServerEvent::Closed,
                    };
                    let closed = matches!(event, AgentServerEvent::Closed);
                    if closed {
                        commands.close();
                        while let Some(command) = commands.recv().await {
                            journal = reply::handle(journal, &mut requests, &sender, command).await?;
                        }
                    }
                    journal = tokio::task::spawn_blocking(move || {
                        journal.append(&serde_json::to_vec(&event)?)?;
                        Ok::<_, io::Error>(journal)
                    })
                    .await
                    .map_err(io::Error::other)??;
                    let _ = progress.send_replace(Ok(journal.position()));
                    if closed {
                        return Ok(journal.position());
                    }
                }
            }
            .await;
            if let Err(error) = &result {
                let _ = progress.send_replace(Err(error.to_string()));
            }
            result
        });
        AgentServerEvents {
            path,
            run_start,
            progress: updates,
            task,
            replies,
        }
    }
}

impl AgentServerEvents {
    /// The Opened record for this app-server instance. Old runs remain in the
    /// same journal for audit but their server requests are no longer live.
    pub fn run_start(&self) -> JournalPosition {
        self.run_start
    }

    pub fn snapshot(&self) -> io::Result<(PathBuf, JournalPosition)> {
        let position = self.progress.borrow().clone().map_err(io::Error::other)?;
        Ok((self.path.clone(), position))
    }

    pub fn source(&self) -> &Path {
        &self.path
    }

    /// Subscribe before reading the durable prefix. Each update follows fsync;
    /// errors end capture and must be surfaced by the host loop.
    pub fn subscribe(&self) -> watch::Receiver<Result<JournalPosition, String>> {
        self.progress.clone()
    }

    /// Call after app-server shutdown. Channel closure is recorded after every
    /// delivered event; no timeout or task abort substitutes for this drain.
    pub async fn finish(self) -> io::Result<(PathBuf, JournalPosition)> {
        let position = self.task.await.map_err(io::Error::other)??;
        Ok((self.path, position))
    }
}
