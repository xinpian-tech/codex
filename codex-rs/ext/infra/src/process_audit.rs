use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use codex_exec_server::ExecProcessEvent;
use codex_exec_server::ExecProcessFuture;
use codex_exec_server::ExecServerError;
use codex_exec_server::PreparedProcessCommand;
use codex_exec_server::ProcessId;
use codex_exec_server::ProcessInputRecorder;
use codex_exec_server::ProcessRecorder;
use codex_exec_server::ProcessRecorderFactory;
use codex_exec_server_protocol::ExecParams;
use codex_exec_server_protocol::ExecResponse;
use codex_exec_server_protocol::JSONRPCErrorError;
use codex_exec_server_protocol::ProcessOutputChunk;
use codex_exec_server_protocol::WriteParams;
use codex_exec_server_protocol::WriteResponse;
use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;

use crate::ProcessActivity;
use crate::StoreAuditIdentity;
use crate::WorkspaceLease;
use crate::WorkspaceOperations;

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
    StartFinished {
        requested_sequence: u64,
        outcome: Result<ExecResponse, JSONRPCErrorError>,
    },
    Prepared {
        #[serde(default)]
        requested_sequence: Option<u64>,
        process_id: ProcessId,
        command: Box<PreparedProcessCommand>,
    },
    InputRequested {
        params: WriteParams,
    },
    InputFinished {
        requested_sequence: u64,
        outcome: Result<WriteResponse, JSONRPCErrorError>,
    },
    Output {
        #[serde(default)]
        requested_sequence: Option<u64>,
        process_id: ProcessId,
        chunk: ProcessOutputChunk,
    },
    Exited {
        #[serde(default)]
        requested_sequence: Option<u64>,
        process_id: ProcessId,
        seq: u64,
        exit_code: i32,
        sandbox_denied: Option<bool>,
    },
    Closed {
        #[serde(default)]
        requested_sequence: Option<u64>,
        process_id: ProcessId,
        seq: u64,
    },
    Failed {
        #[serde(default)]
        requested_sequence: Option<u64>,
        process_id: ProcessId,
        message: String,
    },
}

struct Writer {
    journal: Journal,
    failure: Option<String>,
    activity: ProcessActivity,
}

impl Writer {
    fn append(&mut self, event: &ProcessAuditEvent) -> Result<u64, ExecServerError> {
        if let Some(error) = &self.failure {
            return Err(recording_error(error));
        }
        let result = serde_json::to_vec(event)
            .map_err(io::Error::other)
            .and_then(|bytes| self.journal.append(&bytes));
        let sequence = result.map_err(|error| {
            self.failure = Some(error.to_string());
            recording_error(error)
        })?;
        // The ledger latches unresolved provenance independently; retaining
        // subsequent raw evidence must continue even if reconciliation fails.
        let _ = self.activity.apply(sequence, event);
        Ok(sequence)
    }
}

/// One journal writer per Agent host launch, installed on its local exec backend.
/// Payloads remain on disk; recorders retain only their start-attempt identity.
#[derive(Clone)]
pub struct ProcessAudit {
    writer: Arc<Mutex<Writer>>,
    operations: Option<WorkspaceOperations>,
}

impl ProcessAudit {
    /// Recovers the existing journal before any execution backend starts work.
    pub fn open(
        path: &Path,
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    ) -> io::Result<Self> {
        let mut activity = ProcessActivity::default();
        let mut journal = Journal::open(path, |record| {
            let event: ProcessAuditEvent = serde_json::from_slice(&record.payload)?;
            if let ProcessAuditEvent::Opened {
                identity: previous,
                launch_id: previous_launch,
            } = &event
                && (previous != &identity || previous_launch != &launch_id)
            {
                return Err(io::Error::other("process audit launch binding changed"));
            }
            let _ = activity.apply(record.sequence, &event);
            Ok(())
        })?;
        let opened = ProcessAuditEvent::Opened {
            identity,
            launch_id,
        };
        let sequence = journal.append(&serde_json::to_vec(&opened)?)?;
        let _ = activity.apply(sequence, &opened);
        Ok(Self {
            operations: None,
            writer: Arc::new(Mutex::new(Writer {
                journal,
                failure: None,
                activity,
            })),
        })
    }

    /// Binds the controller's operation registry before the host starts work.
    pub fn with_operations(mut self, operations: WorkspaceOperations) -> Self {
        self.operations = Some(operations);
        self
    }

    /// Check after recorded requests and producers have drained. A final
    /// request-result write can fail after the child's output already closed.
    pub fn check_health(&self) -> io::Result<()> {
        let writer = self
            .writer
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        match &writer.failure {
            Some(error) => Err(io::Error::other(error.clone())),
            None => writer.activity.require_settled(),
        }
    }
}

impl ProcessRecorderFactory for ProcessAudit {
    fn open_input<'a>(
        &'a self,
        params: &'a WriteParams,
    ) -> ExecProcessFuture<'a, Arc<dyn ProcessInputRecorder>> {
        let writer = Arc::clone(&self.writer);
        let params = params.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let requested_sequence = writer
                    .lock()
                    .map_err(recording_error)?
                    .append(&ProcessAuditEvent::InputRequested { params })?;
                Ok(Arc::new(InputRecorder {
                    writer,
                    requested_sequence,
                }) as Arc<dyn ProcessInputRecorder>)
            })
            .await
            .map_err(recording_error)?
        })
    }

    fn prepare_start(
        &self,
        params: &ExecParams,
    ) -> Result<ExecProcessFuture<'static, Arc<dyn ProcessRecorder>>, ExecServerError> {
        let lease = self
            .operations
            .as_ref()
            .map(|operations| operations.reserve(params.metadata.as_ref()))
            .transpose()
            .map_err(recording_error)?;
        let params = params.clone();
        let writer = Arc::clone(&self.writer);
        Ok(Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut state = writer.lock().map_err(recording_error)?;
                let process_id = params.process_id.clone();
                let requested_sequence = state.append(&ProcessAuditEvent::Requested {
                    params: Box::new(params),
                })?;
                let recorder = Arc::new(Recorder {
                    process_id,
                    requested_sequence,
                    writer: Arc::clone(&writer),
                    lease: Mutex::new(lease),
                });
                Ok(recorder as Arc<dyn ProcessRecorder>)
            })
            .await
            .map_err(recording_error)?
        }))
    }
}

struct Recorder {
    process_id: ProcessId,
    requested_sequence: u64,
    writer: Arc<Mutex<Writer>>,
    lease: Mutex<Option<WorkspaceLease>>,
}

struct InputRecorder {
    writer: Arc<Mutex<Writer>>,
    requested_sequence: u64,
}

impl ProcessInputRecorder for InputRecorder {
    fn finish(
        &self,
        outcome: Result<WriteResponse, JSONRPCErrorError>,
    ) -> ExecProcessFuture<'_, ()> {
        let writer = Arc::clone(&self.writer);
        let requested_sequence = self.requested_sequence;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                writer.lock().map_err(recording_error)?.append(
                    &ProcessAuditEvent::InputFinished {
                        requested_sequence,
                        outcome,
                    },
                )?;
                Ok(())
            })
            .await
            .map_err(recording_error)?
        })
    }
}

impl Recorder {
    fn append(&self, event: ProcessAuditEvent) -> ExecProcessFuture<'_, ()> {
        let writer = Arc::clone(&self.writer);
        let release = matches!(
            &event,
            ProcessAuditEvent::Closed { .. }
                | ProcessAuditEvent::StartFinished {
                    outcome: Err(_),
                    ..
                }
        );
        Box::pin(async move {
            tokio::task::spawn_blocking(move || -> Result<(), ExecServerError> {
                writer.lock().map_err(recording_error)?.append(&event)?;
                Ok(())
            })
            .await
            .map_err(recording_error)??;
            if release {
                self.lease.lock().map_err(recording_error)?.take();
            }
            Ok(())
        })
    }
}

impl ProcessRecorder for Recorder {
    fn start_finished(
        &self,
        outcome: Result<ExecResponse, JSONRPCErrorError>,
    ) -> ExecProcessFuture<'_, ()> {
        self.append(ProcessAuditEvent::StartFinished {
            requested_sequence: self.requested_sequence,
            outcome,
        })
    }

    fn prepared(&self, command: PreparedProcessCommand) -> ExecProcessFuture<'_, ()> {
        self.append(ProcessAuditEvent::Prepared {
            requested_sequence: Some(self.requested_sequence),
            process_id: self.process_id.clone(),
            command: Box::new(command),
        })
    }

    fn record(&self, event: ExecProcessEvent) -> ExecProcessFuture<'_, ()> {
        let process_id = self.process_id.clone();
        let requested_sequence = Some(self.requested_sequence);
        let event = match event {
            ExecProcessEvent::Output(chunk) => ProcessAuditEvent::Output {
                process_id,
                requested_sequence,
                chunk,
            },
            ExecProcessEvent::Exited {
                seq,
                exit_code,
                sandbox_denied,
            } => ProcessAuditEvent::Exited {
                process_id,
                requested_sequence,
                seq,
                exit_code,
                sandbox_denied,
            },
            ExecProcessEvent::Closed { seq } => ProcessAuditEvent::Closed {
                process_id,
                requested_sequence,
                seq,
            },
            ExecProcessEvent::Failed(message) => ProcessAuditEvent::Failed {
                process_id,
                requested_sequence,
                message,
            },
        };
        self.append(event)
    }
}

fn recording_error(error: impl std::fmt::Display) -> ExecServerError {
    ExecServerError::Protocol(format!("process audit: {error}"))
}
