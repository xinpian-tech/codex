use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_extension_api::ExtensionFuture;
use codex_extension_api::ModelRequestContributor;
use codex_extension_api::ModelRequestInput;
use codex_extension_api::ModelRequestKind;
use codex_extension_api::ModelRequestObservation;
use codex_extension_api::ModelResponseInterceptor;
use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_protocol::models::ResponseItem;
use serde::Deserialize;
use serde::Serialize;

use crate::StoreAuditIdentity;

mod reader;
mod response;
pub use reader::ModelInputAuditPage;
pub use reader::ModelInputAuditRecord;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModelInputAuditEvent {
    Opened {
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    },
    Prepared {
        attempt_id: MessageId,
        thread_id: String,
        turn_id: Option<String>,
        model: String,
        warmup: bool,
        previous_response_id: Option<String>,
        input: Vec<ResponseItem>,
    },
    Created {
        attempt_id: MessageId,
        response_id: Option<String>,
    },
    Completed {
        attempt_id: MessageId,
        response_id: String,
        token_usage: Option<codex_protocol::protocol::TokenUsage>,
        usage_metadata: Option<codex_protocol::ResponseUsageMetadata>,
        end_turn: Option<bool>,
    },
    ServerModel {
        attempt_id: MessageId,
        model: String,
    },
    StreamFailed {
        attempt_id: MessageId,
        error: String,
    },
    StreamEnded {
        attempt_id: MessageId,
    },
}

/// Durable progress and terminal writer state for event-driven consumers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelInputAuditState {
    Running { position: JournalPosition },
    Closed { position: JournalPosition },
    Failed { error: String },
}

/// Records final transport inputs before submission. These records are evidence
/// of preparation, not provider receipt. Warmup and WebSocket deltas remain
/// explicit so recovery does not mistake them for full generation requests.
#[derive(Clone)]
pub struct ModelInputAudit {
    writer: Arc<Mutex<Writer>>,
    changes: tokio::sync::watch::Receiver<ModelInputAuditState>,
}

struct Writer {
    path: PathBuf,
    journal: Journal,
    closed: bool,
    changed: tokio::sync::watch::Sender<ModelInputAuditState>,
}

impl Writer {
    fn append(&mut self, event: &ModelInputAuditEvent) -> io::Result<()> {
        if let ModelInputAuditState::Failed { error } = &*self.changed.borrow() {
            return Err(io::Error::other(error.clone()));
        }
        match serde_json::to_vec(event)
            .map_err(io::Error::other)
            .and_then(|bytes| self.journal.append(&bytes))
        {
            Ok(_) => {
                self.changed.send_replace(ModelInputAuditState::Running {
                    position: self.journal.position(),
                });
                Ok(())
            }
            Err(error) => {
                self.changed.send_replace(ModelInputAuditState::Failed {
                    error: error.to_string(),
                });
                Err(error)
            }
        }
    }
}

impl std::fmt::Debug for ModelInputAudit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelInputAudit")
            .finish_non_exhaustive()
    }
}

impl ModelInputAudit {
    /// Subscribe before scanning, then wait for changes after catching up.
    /// Notifications follow durable writes; consumers read evidence by cursor.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<ModelInputAuditState> {
        self.changes.clone()
    }

    /// Samples the locally durable prefix. ProducerFinished archive jobs require
    /// close first; a live snapshot is only a prefix, not producer completion.
    pub fn snapshot(&self) -> io::Result<(PathBuf, JournalPosition)> {
        let writer = self
            .writer
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if let ModelInputAuditState::Failed { error } = &*writer.changed.borrow() {
            return Err(io::Error::other(error.clone()));
        }
        Ok((writer.path.clone(), writer.journal.position()))
    }

    /// Open on a blocking worker before registering the request contributor.
    pub fn open(
        path: &Path,
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    ) -> io::Result<Self> {
        let mut journal = Journal::open(path, |record| {
            if let ModelInputAuditEvent::Opened {
                identity: previous,
                launch_id: previous_launch,
            } = serde_json::from_slice(&record.payload)?
                && (previous != identity || previous_launch != launch_id)
            {
                return Err(io::Error::other("model input audit launch changed"));
            }
            Ok(())
        })?;
        journal.append(&serde_json::to_vec(&ModelInputAuditEvent::Opened {
            identity,
            launch_id,
        })?)?;
        let (changed, changes) = tokio::sync::watch::channel(ModelInputAuditState::Running {
            position: journal.position(),
        });
        Ok(Self {
            changes,
            writer: Arc::new(Mutex::new(Writer {
                path: path.canonicalize()?,
                journal,
                closed: false,
                changed,
            })),
        })
    }

    /// Call after inference shutdown. All accepted writes finish under the
    /// same lock; later observations fail before transport submission.
    pub async fn close(&self) -> io::Result<(PathBuf, JournalPosition)> {
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let mut writer = writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            writer.closed = true;
            if let ModelInputAuditState::Failed { error } = &*writer.changed.borrow() {
                return Err(io::Error::other(error.clone()));
            }
            writer.changed.send_replace(ModelInputAuditState::Closed {
                position: writer.journal.position(),
            });
            Ok((writer.path.clone(), writer.journal.position()))
        })
        .await
        .map_err(io::Error::other)?
    }
}

impl ModelRequestContributor for ModelInputAudit {
    fn request(&self, _input: ModelRequestInput<'_>) -> Option<Box<dyn ModelResponseInterceptor>> {
        None
    }

    fn observe<'a>(
        &'a self,
        input: ModelRequestObservation<'a>,
    ) -> ExtensionFuture<'a, io::Result<Option<Box<dyn ModelResponseInterceptor>>>> {
        Box::pin(async move {
            let attempt_id = MessageId::new();
            let event = ModelInputAuditEvent::Prepared {
                attempt_id,
                thread_id: input.thread_id.to_owned(),
                turn_id: input.turn_id.map(str::to_owned),
                model: input.model.to_owned(),
                warmup: input.kind == ModelRequestKind::Warmup,
                previous_response_id: input.previous_response_id.map(str::to_owned),
                input: input.input.to_vec(),
            };
            let writer = Arc::clone(&self.writer);
            tokio::task::spawn_blocking(move || {
                let mut writer = writer
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                if writer.closed {
                    return Err(io::Error::other("model input audit is closed"));
                }
                writer.append(&event)
            })
            .await
            .map_err(io::Error::other)??;
            Ok(Some(Box::new(response::ResponseAudit {
                audit: self.clone(),
                attempt_id,
            }) as Box<dyn ModelResponseInterceptor>))
        })
    }
}
