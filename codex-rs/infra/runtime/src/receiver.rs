use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;

use codex_infra_protocol::MessageId;
use codex_infra_tmux::GatewayListener;
use codex_infra_tmux::TransportFrame;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::watch;
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
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl GatewayReception {
    pub fn start(listener: GatewayListener, capacity: NonZeroUsize) -> io::Result<Self> {
        let endpoint = listener.endpoint()?;
        let (sender, events) = mpsc::channel(capacity.get());
        let (stop, _) = watch::channel(false);
        let task = tokio::spawn(receive_connections(listener, sender, stop.clone()));
        Ok(Self {
            endpoint,
            events,
            stop,
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

    /// Stops new accepts and frame reads. Drain `next_event` through None so
    /// decoded frames waiting on the bounded channel and final close events can
    /// finish handoff. Unacknowledged messages remain in sending hosts.
    pub fn stop_reading(&self) {
        self.stop.send_replace(true);
    }
}

impl Drop for GatewayReception {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn receive_connections(
    listener: GatewayListener,
    sender: mpsc::Sender<ReceptionEvent>,
    stop: watch::Sender<bool>,
) {
    let mut stopped = stop.subscribe();
    let mut readers = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = async { let _ = stopped.wait_for(|stopped| *stopped).await; } => break,
            _ = sender.closed() => return,
            finished = readers.join_next(), if !readers.is_empty() => {
                if let Some(Err(error)) = finished
                    && sender.send(ReceptionEvent::WorkerFailed { error: error.to_string() }).await.is_err()
                {
                    return;
                }
            }
            accepted = listener.accept() => {
                let (connection, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        let _ = sender.send(ReceptionEvent::ListenerFailed { error: error.to_string() }).await;
                        stop.send_replace(true);
                        break;
                    }
                };
                let connection_id = MessageId::new();
                if sender.send(ReceptionEvent::Opened { connection_id, peer }).await.is_err() {
                    return;
                }
                let output = sender.clone();
                let mut reader_stopped = stop.subscribe();
                readers.spawn(async move {
                    let (_writer, mut reader) = connection.split();
                    let mut sequence = 0_u64;
                    loop {
                        let received = tokio::select! {
                            biased;
                            _ = async { let _ = reader_stopped.wait_for(|stopped| *stopped).await; } => {
                                Err(io::Error::other("network reader stopped"))
                            }
                            received = reader.receive() => received,
                        };
                        let frame = match received {
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
        }
    }
    drop(listener);
    // Readers finish any already decoded frame and their close record before
    // dropping the channel. The owner drains events concurrently into its spool.
    while let Some(finished) = readers.join_next().await {
        if let Err(error) = finished
            && sender
                .send(ReceptionEvent::WorkerFailed {
                    error: error.to_string(),
                })
                .await
                .is_err()
        {
            return;
        }
    }
}
