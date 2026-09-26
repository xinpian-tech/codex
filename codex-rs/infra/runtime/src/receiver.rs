use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;

use codex_infra_protocol::MessageId;
use codex_infra_tmux::GatewayListener;
use codex_infra_tmux::TransportFrame;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReceptionEvent {
    Opened {
        connection_id: MessageId,
        peer: SocketAddr,
    },
    Frame {
        connection_id: MessageId,
        sequence: u64,
        frame: TransportFrame,
    },
    Closed {
        connection_id: MessageId,
        frames: u64,
        error: String,
    },
    ListenerFailed {
        error: String,
    },
    WorkerFailed {
        error: String,
    },
}

/// Owns independent TCP readers and a bounded handoff to the machine's disk
/// writer. No receipt is generated here; the receiving Agent must persist its
/// inbox before it can acknowledge semantic delivery through its own stdout.
pub struct GatewayReception {
    endpoint: SocketAddr,
    events: mpsc::Receiver<ReceptionEvent>,
    task: JoinHandle<()>,
}

impl GatewayReception {
    pub fn start(listener: GatewayListener, capacity: NonZeroUsize) -> io::Result<Self> {
        let endpoint = listener.endpoint()?;
        let (sender, events) = mpsc::channel(capacity.get());
        let task = tokio::spawn(receive_connections(listener, sender));
        Ok(Self {
            endpoint,
            events,
            task,
        })
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    /// Canceling this wait leaves the connection readers running. The caller
    /// journals each event and stages frames before asking for more input.
    pub async fn next_event(&mut self) -> Option<ReceptionEvent> {
        self.events.recv().await
    }

    pub fn try_event(&mut self) -> Result<ReceptionEvent, mpsc::error::TryRecvError> {
        self.events.try_recv()
    }

    /// Stops network reads. Drain `next_event` through None to persist events
    /// already handed off; unacknowledged messages remain in sending hosts.
    pub fn stop_reading(&self) {
        self.task.abort();
    }
}

impl Drop for GatewayReception {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn receive_connections(listener: GatewayListener, sender: mpsc::Sender<ReceptionEvent>) {
    let mut readers = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (connection, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        let _ = sender.send(ReceptionEvent::ListenerFailed { error: error.to_string() }).await;
                        return;
                    }
                };
                let connection_id = MessageId::new();
                if sender.send(ReceptionEvent::Opened { connection_id, peer }).await.is_err() {
                    return;
                }
                let output = sender.clone();
                readers.spawn(async move {
                    let (_writer, mut reader) = connection.split();
                    let mut sequence = 0_u64;
                    loop {
                        let frame = match reader.receive().await {
                            Ok(frame) => frame,
                            Err(error) => {
                                let _ = output.send(ReceptionEvent::Closed {
                                    connection_id, frames: sequence, error: error.to_string(),
                                }).await;
                                return;
                            }
                        };
                        if output.send(ReceptionEvent::Frame { connection_id, sequence, frame }).await.is_err() {
                            return;
                        }
                        match sequence.checked_add(1) {
                            Some(next) => sequence = next,
                            None => {
                                let _ = output.send(ReceptionEvent::Closed {
                                    connection_id, frames: sequence, error: "connection sequence exhausted".to_owned(),
                                }).await;
                                return;
                            }
                        }
                    }
                });
            }
            finished = readers.join_next(), if !readers.is_empty() => {
                if let Some(Err(error)) = finished
                    && sender.send(ReceptionEvent::WorkerFailed { error: error.to_string() }).await.is_err()
                {
                    return;
                }
            }
            _ = sender.closed() => return,
        }
    }
}
