use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use codex_extension_api::ToolCallOutcome;
use codex_extension_api::ToolCallSource;
use codex_extension_api::ToolExecutionFuture;
use codex_extension_api::ToolExecutionInput;
use codex_extension_api::ToolExecutionKind;
use codex_extension_api::ToolExecutionOrigin;
use codex_extension_api::ToolFinishInput;
use codex_extension_api::ToolLifecycleContributor;
use codex_extension_api::ToolLifecycleFuture;
use codex_extension_api::ToolPayload;
use codex_extension_api::ToolStartInput;
use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::StoreAuditIdentity;
use crate::ToolActivity;
use crate::ToolWorkspace;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolOperation {
    pub thread_id: String,
    pub turn_id: String,
    pub call_id: String,
    pub tool_name: String,
    pub source: ToolOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolOrigin {
    Direct,
    CodeMode {
        cell_id: String,
        runtime_tool_call_id: String,
    },
}

impl From<ToolCallSource> for ToolOrigin {
    fn from(source: ToolCallSource) -> Self {
        match source {
            ToolCallSource::Direct => Self::Direct,
            ToolCallSource::CodeMode {
                cell_id,
                runtime_tool_call_id,
            } => Self::CodeMode {
                cell_id,
                runtime_tool_call_id,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolOutcome {
    Completed { success: bool },
    Blocked,
    Failed { handler_executed: bool },
    Aborted,
}

/// Tool completion describes the handler, not any child process it yielded.
/// A finish can exist without a start when cancellation or blocking wins first.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolAuditEvent {
    Opened {
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    },
    Admitted {
        operation: ToolOperation,
        #[serde(default)]
        execution_kind: Option<ToolExecutionKind>,
        #[serde(default)]
        origin: Option<ToolExecutionOrigin>,
    },
    Started {
        operation: ToolOperation,
        root_turn_id: Option<String>,
        originating_item_id: Option<String>,
        payload: RecordedToolPayload,
    },
    Finished {
        operation: ToolOperation,
        outcome: ToolOutcome,
    },
    McpHookResult {
        operation: ToolOperation,
        outcome: Result<codex_hooks::HookMcpOutput, String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RecordedToolPayload {
    Function { arguments: String },
    ToolSearch { arguments: Value },
    Custom { input: String },
}

struct Writer {
    journal: Journal,
    failure: Option<String>,
    activity: ToolActivity,
    workspace: Option<ToolWorkspace>,
}

/// Durable operation provenance before hooks and at handler completion.
/// Admission failures stop dispatch. Later observation failures are retained
/// for checkpoint/finalization checks because the tool may already be running.
#[derive(Clone)]
pub struct ToolAudit {
    pub(crate) path: std::path::PathBuf,
    pub(crate) identity: StoreAuditIdentity,
    pub(crate) launch_id: MessageId,
    writer: Arc<Mutex<Writer>>,
    pending: Arc<AtomicUsize>,
    append_order: Arc<tokio::sync::Semaphore>,
}

struct PendingWrite(Arc<AtomicUsize>);

impl Drop for PendingWrite {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ToolAudit {
    pub fn open(
        path: &Path,
        identity: StoreAuditIdentity,
        launch_id: MessageId,
    ) -> io::Result<Self> {
        let mut activity = ToolActivity::default();
        let mut journal = Journal::open(path, |record| {
            let event: ToolAuditEvent = serde_json::from_slice(&record.payload)?;
            if let ToolAuditEvent::Opened {
                identity: previous,
                launch_id: previous_launch,
            } = &event
                && (previous != &identity || previous_launch != &launch_id)
            {
                return Err(io::Error::other("tool audit launch binding changed"));
            }
            let _ = activity.apply(record.sequence, &event);
            Ok(())
        })?;
        let opened = ToolAuditEvent::Opened {
            identity: identity.clone(),
            launch_id,
        };
        let sequence = journal.append(&serde_json::to_vec(&opened)?)?;
        let _ = activity.apply(sequence, &opened);
        Ok(Self {
            path: path.canonicalize()?,
            identity,
            launch_id,
            pending: Arc::new(AtomicUsize::new(0)),
            append_order: Arc::new(tokio::sync::Semaphore::new(/*permits*/ 1)),
            writer: Arc::new(Mutex::new(Writer {
                journal,
                failure: None,
                activity,
                workspace: None,
            })),
        })
    }

    /// Bind before starting the host. Its ProcessAudit must use the same
    /// controller's operations to retain background process ownership.
    pub fn with_workspace(self, workspace: ToolWorkspace) -> io::Result<Self> {
        self.writer
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?
            .workspace = Some(workspace);
        Ok(self)
    }

    pub fn workspace(&self) -> io::Result<Option<ToolWorkspace>> {
        Ok(self
            .writer
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?
            .workspace
            .clone())
    }

    /// Call after tool dispatch is quiescent. Pending disk writes are not yet
    /// healthy, even when their lifecycle callback was cancelled by its caller.
    pub fn check_health(&self) -> io::Result<()> {
        self.settled_position().map(|_| ())
    }

    /// Returns the fsync-acknowledged boundary after all admitted operations
    /// settle. The caller keeps dispatch quiescent through archive publication.
    pub fn settled_position(&self) -> io::Result<codex_infra_state::JournalPosition> {
        if self.pending.load(Ordering::SeqCst) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "tool audit writes pending",
            ));
        }
        let writer = self
            .writer
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        match &writer.failure {
            Some(error) => Err(io::Error::other(error.clone())),
            None => {
                writer.activity.require_settled()?;
                Ok(writer.journal.position())
            }
        }
    }

    pub(crate) async fn append(&self, event: io::Result<ToolAuditEvent>) -> Result<(), String> {
        let writer = Arc::clone(&self.writer);
        self.pending.fetch_add(1, Ordering::SeqCst);
        let pending = PendingWrite(Arc::clone(&self.pending));
        // Acquire before handing work to the blocking pool: cancellation of
        // a start callback must not let its finish write overtake the start.
        let order = Arc::clone(&self.append_order).acquire_owned().await;
        let result = tokio::task::spawn_blocking(move || {
            let _pending = pending;
            let mut writer = writer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let result = event.and_then(|event| {
                let _order = order.map_err(io::Error::other)?;
                if matches!(&event, ToolAuditEvent::Admitted { .. })
                    && let Some(error) = &writer.failure
                {
                    return Err(io::Error::other(error.clone()));
                }
                let bytes = serde_json::to_vec(&event)?;
                let sequence = writer.journal.append(&bytes)?;
                let reconciled = writer.activity.apply(sequence, &event);
                if matches!(
                    &event,
                    ToolAuditEvent::Admitted { .. } | ToolAuditEvent::McpHookResult { .. }
                ) {
                    reconciled?;
                } else if let ToolAuditEvent::Finished { operation, .. } = &event {
                    reconciled?;
                    if let Some(error) = &writer.failure {
                        return Err(io::Error::other(error.clone()));
                    }
                    if let Some(workspace) = &writer.workspace {
                        workspace.finish(operation)?;
                    }
                }
                Ok(sequence)
            });
            if let Err(error) = &result {
                writer.failure.get_or_insert_with(|| error.to_string());
            }
            result.map(|_| ())
        })
        .await;
        match result {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(error) => {
                self.writer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .failure
                    .get_or_insert_with(|| error.to_string());
                Err(error.to_string())
            }
        }
    }
}

impl ToolLifecycleContributor for ToolAudit {
    fn acquire_tool_execution<'a>(
        &'a self,
        input: ToolExecutionInput<'a>,
    ) -> ToolExecutionFuture<'a> {
        Box::pin(async move {
            let operation = ToolOperation {
                thread_id: input.thread_id.to_owned(),
                turn_id: input.turn_id.to_owned(),
                call_id: input.call_id.to_owned(),
                tool_name: input.tool_name.to_string(),
                source: input.source.clone().into(),
            };
            self.append(Ok(ToolAuditEvent::Admitted {
                execution_kind: Some(input.kind),
                origin: input.origin.cloned(),
                operation: operation.clone(),
            }))
            .await?;
            match self.workspace().map_err(|error| error.to_string())? {
                Some(workspace) => workspace
                    .acquire(&input, operation)
                    .await
                    .map(|lease| {
                        lease.map(|lease| {
                            Box::new(lease) as Box<dyn codex_extension_api::ToolExecutionLease>
                        })
                    })
                    .map_err(|error| error.to_string()),
                None => Ok(None),
            }
        })
    }

    fn on_tool_start<'a>(&'a self, input: ToolStartInput<'a>) -> ToolLifecycleFuture<'a> {
        Box::pin(async move {
            let payload = match input.payload {
                ToolPayload::Function { arguments } => Ok(RecordedToolPayload::Function {
                    arguments: arguments.clone(),
                }),
                ToolPayload::ToolSearch { arguments } => serde_json::to_value(arguments)
                    .map(|arguments| RecordedToolPayload::ToolSearch { arguments })
                    .map_err(io::Error::other),
                ToolPayload::Custom { input } => Ok(RecordedToolPayload::Custom {
                    input: input.clone(),
                }),
            };
            let _ = self
                .append(payload.map(|payload| ToolAuditEvent::Started {
                    operation: ToolOperation {
                        thread_id: input.thread_store.level_id().to_owned(),
                        turn_id: input.turn_id.to_owned(),
                        call_id: input.call_id.to_owned(),
                        tool_name: input.tool_name.to_string(),
                        source: input.source.into(),
                    },
                    root_turn_id: input.root_turn_id.map(str::to_owned),
                    originating_item_id: input.originating_item_id.map(ToString::to_string),
                    payload,
                }))
                .await;
        })
    }

    fn on_tool_finish<'a>(&'a self, input: ToolFinishInput<'a>) -> ToolLifecycleFuture<'a> {
        Box::pin(async move {
            let outcome = match input.outcome {
                ToolCallOutcome::Completed { success } => ToolOutcome::Completed { success },
                ToolCallOutcome::Blocked => ToolOutcome::Blocked,
                ToolCallOutcome::Failed { handler_executed } => {
                    ToolOutcome::Failed { handler_executed }
                }
                ToolCallOutcome::Aborted => ToolOutcome::Aborted,
            };
            let _ = self
                .append(Ok(ToolAuditEvent::Finished {
                    operation: ToolOperation {
                        thread_id: input.thread_store.level_id().to_owned(),
                        turn_id: input.turn_id.to_owned(),
                        call_id: input.call_id.to_owned(),
                        tool_name: input.tool_name.to_string(),
                        source: input.source.into(),
                    },
                    outcome,
                }))
                .await;
        })
    }
}
