use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_app_server::in_process::InProcessClientSender;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::StoreAuditIdentity;

/// A transport failure or interrupted request needs history reconciliation, not
/// automatic resubmission. A protocol error is a recorded Reply like success.
#[derive(Clone, Debug)]
pub enum AgentRpcOutcome {
    Reply(Result<Value, JSONRPCErrorError>),
    Uncertain {
        request_id: RequestId,
        error: Option<String>,
    },
}

/// Owns serialized app-server requests through durable response recording.
#[derive(Clone)]
pub struct AgentRpc {
    writer: Arc<Mutex<Writer>>,
    closed: Arc<tokio::sync::Mutex<bool>>,
}

struct Writer {
    path: PathBuf,
    journal: Journal,
    requests: BTreeMap<String, RecordedRequest>,
}

struct RecordedRequest {
    digest: blake3::Hash,
    outcome: RecordedOutcome,
}

enum RecordedOutcome {
    Pending,
    Reply(JournalPosition),
    TransportFailure(String),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RpcEvent {
    Opened {
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    },
    Requested {
        request: Box<ClientRequest>,
    },
    Replied {
        request_id: RequestId,
        response: Result<Value, JSONRPCErrorError>,
    },
    TransportFailed {
        request_id: RequestId,
        error: String,
    },
}

impl AgentRpc {
    /// Open on a blocking worker before starting the embedded app-server.
    pub fn open(
        path: &Path,
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    ) -> io::Result<Self> {
        let mut requests = BTreeMap::new();
        let mut journal = Journal::open(path, |record| {
            let event: RpcEvent = serde_json::from_slice(&record.payload)?;
            if let RpcEvent::Opened {
                identity: previous,
                launch_id: previous_launch,
            } = &event
                && (previous != &identity || *previous_launch != launch_id)
            {
                return Err(io::Error::other("Agent RPC launch binding changed"));
            }
            apply(&mut requests, &event, record.position)
        })?;
        journal.append(&serde_json::to_vec(&RpcEvent::Opened {
            identity,
            launch_id,
        })?)?;
        Ok(Self {
            writer: Arc::new(Mutex::new(Writer {
                path: path.canonicalize()?,
                journal,
                requests,
            })),
            closed: Arc::default(),
        })
    }

    /// Reusing an ID returns its durable reply, or Uncertain if the previous
    /// attempt has no confirmed response. Different payloads need different IDs.
    /// Cancellation drops only the waiter; the owned request finishes recording.
    pub async fn request(
        &self,
        sender: InProcessClientSender,
        request: ClientRequest,
    ) -> io::Result<AgentRpcOutcome> {
        let rpc = self.clone();
        tokio::spawn(async move {
            let closed = Arc::clone(&rpc.closed).lock_owned().await;
            if *closed {
                return Err(io::Error::other("Agent RPC admission is closed"));
            }
            let writer = Arc::clone(&rpc.writer);
            let prepared = request.clone();
            let replay = tokio::task::spawn_blocking(move || {
                let mut writer = writer
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                writer.begin(prepared)
            })
            .await
            .map_err(io::Error::other)??;
            if let Some(outcome) = replay {
                return Ok(outcome);
            }
            let request_id = request.id().clone();
            let response = sender.request(request).await;
            tokio::task::spawn_blocking(move || {
                let mut writer = rpc
                    .writer
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                let (event, outcome) = match response {
                    Ok(response) => (
                        RpcEvent::Replied {
                            request_id,
                            response: response.clone(),
                        },
                        AgentRpcOutcome::Reply(response),
                    ),
                    Err(error) => {
                        let error = error.to_string();
                        (
                            RpcEvent::TransportFailed {
                                request_id: request_id.clone(),
                                error: error.clone(),
                            },
                            AgentRpcOutcome::Uncertain {
                                request_id,
                                error: Some(error),
                            },
                        )
                    }
                };
                writer.append(event)?;
                Ok(outcome)
            })
            .await
            .map_err(io::Error::other)?
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Stops admission and waits for the admitted request's journal writes.
    pub async fn close(&self) -> io::Result<()> {
        let closed = Arc::clone(&self.closed);
        tokio::spawn(async move {
            *closed.lock().await = true;
        })
        .await
        .map_err(io::Error::other)
    }

    /// Returns a durable prefix; close admission first when using it as a final
    /// producer position. Uncertain requests remain explicitly recorded.
    pub fn snapshot(&self) -> io::Result<(PathBuf, JournalPosition)> {
        let writer = self
            .writer
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok((writer.path.clone(), writer.journal.position()))
    }
}

impl Writer {
    fn begin(&mut self, request: ClientRequest) -> io::Result<Option<AgentRpcOutcome>> {
        let key = serde_json::to_string(request.id())?;
        let digest = blake3::hash(&serde_json::to_vec(&request)?);
        if let Some(record) = self.requests.get(&key) {
            if record.digest != digest {
                return Err(io::Error::other(
                    "Agent RPC request ID reused with different input",
                ));
            }
            return Ok(Some(match &record.outcome {
                RecordedOutcome::Pending => AgentRpcOutcome::Uncertain {
                    request_id: request.id().clone(),
                    error: None,
                },
                RecordedOutcome::TransportFailure(error) => AgentRpcOutcome::Uncertain {
                    request_id: request.id().clone(),
                    error: Some(error.clone()),
                },
                RecordedOutcome::Reply(position) => {
                    let record = JournalReader::open(&self.path, *position)?
                        .next_record()?
                        .ok_or_else(|| io::Error::other("Agent RPC reply record missing"))?;
                    let RpcEvent::Replied {
                        request_id,
                        response,
                    } = serde_json::from_slice(&record.payload)?
                    else {
                        return Err(io::Error::other(
                            "Agent RPC reply position has another event",
                        ));
                    };
                    if &request_id != request.id() {
                        return Err(io::Error::other("Agent RPC reply ID differs"));
                    }
                    AgentRpcOutcome::Reply(response)
                }
            }));
        }
        self.append(RpcEvent::Requested {
            request: Box::new(request),
        })?;
        Ok(None)
    }

    fn append(&mut self, event: RpcEvent) -> io::Result<()> {
        let position = self.journal.position();
        self.journal.append(&serde_json::to_vec(&event)?)?;
        apply(&mut self.requests, &event, position)
    }
}

fn apply(
    requests: &mut BTreeMap<String, RecordedRequest>,
    event: &RpcEvent,
    position: JournalPosition,
) -> io::Result<()> {
    match event {
        RpcEvent::Opened { .. } => {}
        RpcEvent::Requested { request } => {
            let key = serde_json::to_string(request.id())?;
            if requests.contains_key(&key) {
                return Err(io::Error::other("duplicate Agent RPC request record"));
            }
            requests.insert(
                key,
                RecordedRequest {
                    digest: blake3::hash(&serde_json::to_vec(request)?),
                    outcome: RecordedOutcome::Pending,
                },
            );
        }
        RpcEvent::Replied { request_id, .. } | RpcEvent::TransportFailed { request_id, .. } => {
            let record = requests
                .get_mut(&serde_json::to_string(request_id)?)
                .ok_or_else(|| io::Error::other("Agent RPC response has no request"))?;
            if !matches!(record.outcome, RecordedOutcome::Pending) {
                return Err(io::Error::other("Agent RPC request already has an outcome"));
            }
            record.outcome = match event {
                RpcEvent::Replied { .. } => RecordedOutcome::Reply(position),
                RpcEvent::TransportFailed { error, .. } => {
                    RecordedOutcome::TransportFailure(error.clone())
                }
                RpcEvent::Opened { .. } | RpcEvent::Requested { .. } => unreachable!(),
            };
        }
    }
    Ok(())
}
