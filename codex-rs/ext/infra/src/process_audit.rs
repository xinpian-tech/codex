use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use codex_exec_server::ExecProcessEvent;
use codex_exec_server::ExecProcessFuture;
use codex_exec_server::ExecServerError;
use codex_exec_server::ProcessId;
use codex_exec_server::ProcessRecorder;
use codex_exec_server::ProcessRecorderFactory;
use codex_exec_server_protocol::ExecParams;
use codex_exec_server_protocol::ProcessOutputChunk;
use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;

use crate::StoreAuditIdentity;

/// Raw requested execution and producer events. Requested is not evidence that
/// a process was created: retries and unsuccessful starts are retained too.
/// Exited records status; only Closed records that all output has drained.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcessAuditEvent {
    Opened {
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    },
    Requested {
        params: Box<ExecParams>,
    },
    Output {
        process_id: ProcessId,
        chunk: ProcessOutputChunk,
    },
    Exited {
        process_id: ProcessId,
        seq: u64,
        exit_code: i32,
        sandbox_denied: Option<bool>,
    },
    Closed {
        process_id: ProcessId,
        seq: u64,
    },
    Failed {
        process_id: ProcessId,
        message: String,
    },
}

struct Writer {
    journal: Journal,
    recorders: BTreeMap<ProcessId, Weak<Recorder>>,
}

/// One journal writer per Agent host launch, installed on its local exec backend.
/// Payloads remain on disk; the index only references live process recorders.
#[derive(Clone)]
pub struct ProcessAudit {
    writer: Arc<Mutex<Writer>>,
}

impl ProcessAudit {
    /// Recovers the existing journal before any execution backend starts work.
    pub fn open(
        path: &Path,
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    ) -> io::Result<Self> {
        let mut journal = Journal::open(path, |record| {
            let event: ProcessAuditEvent = serde_json::from_slice(&record.payload)?;
            if let ProcessAuditEvent::Opened {
                identity: previous,
                launch_id: previous_launch,
            } = event
                && (previous != identity || previous_launch != launch_id)
            {
                return Err(io::Error::other("process audit launch binding changed"));
            }
            Ok(())
        })?;
        journal.append(&serde_json::to_vec(&ProcessAuditEvent::Opened {
            identity,
            launch_id,
        })?)?;
        Ok(Self {
            writer: Arc::new(Mutex::new(Writer {
                journal,
                recorders: BTreeMap::new(),
            })),
        })
    }
}

impl ProcessRecorderFactory for ProcessAudit {
    fn open<'a>(
        &'a self,
        params: &'a ExecParams,
    ) -> ExecProcessFuture<'a, Arc<dyn ProcessRecorder>> {
        Box::pin(async move {
            let params = params.clone();
            let writer = Arc::clone(&self.writer);
            tokio::task::spawn_blocking(move || {
                let mut state = writer.lock().map_err(recording_error)?;
                let process_id = params.process_id.clone();
                let bytes = serde_json::to_vec(&ProcessAuditEvent::Requested {
                    params: Box::new(params),
                })
                .map_err(recording_error)?;
                state.journal.append(&bytes).map_err(recording_error)?;
                state
                    .recorders
                    .retain(|_, recorder| recorder.strong_count() != 0);
                if let Some(recorder) = state.recorders.get(&process_id).and_then(Weak::upgrade) {
                    return Ok(recorder as Arc<dyn ProcessRecorder>);
                }
                let recorder = Arc::new(Recorder {
                    process_id: process_id.clone(),
                    writer: Arc::clone(&writer),
                });
                state
                    .recorders
                    .insert(process_id, Arc::downgrade(&recorder));
                Ok(recorder as Arc<dyn ProcessRecorder>)
            })
            .await
            .map_err(recording_error)?
        })
    }
}

struct Recorder {
    process_id: ProcessId,
    writer: Arc<Mutex<Writer>>,
}

impl ProcessRecorder for Recorder {
    fn record(&self, event: ExecProcessEvent) -> ExecProcessFuture<'_, ()> {
        Box::pin(async move {
            let process_id = self.process_id.clone();
            let event = match event {
                ExecProcessEvent::Output(chunk) => ProcessAuditEvent::Output { process_id, chunk },
                ExecProcessEvent::Exited {
                    seq,
                    exit_code,
                    sandbox_denied,
                } => ProcessAuditEvent::Exited {
                    process_id,
                    seq,
                    exit_code,
                    sandbox_denied,
                },
                ExecProcessEvent::Closed { seq } => ProcessAuditEvent::Closed { process_id, seq },
                ExecProcessEvent::Failed(message) => ProcessAuditEvent::Failed {
                    process_id,
                    message,
                },
            };
            let writer = Arc::clone(&self.writer);
            tokio::task::spawn_blocking(move || {
                let bytes = serde_json::to_vec(&event).map_err(recording_error)?;
                writer
                    .lock()
                    .map_err(recording_error)?
                    .journal
                    .append(&bytes)
                    .map_err(recording_error)?;
                Ok(())
            })
            .await
            .map_err(recording_error)?
        })
    }
}

fn recording_error(error: impl std::fmt::Display) -> ExecServerError {
    ExecServerError::Protocol(format!("process audit: {error}"))
}
