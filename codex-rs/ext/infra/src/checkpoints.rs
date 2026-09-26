use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_state::CheckpointCoordinator;
use codex_infra_state::CheckpointKind;
use codex_infra_state::CheckpointPhase;
use codex_infra_state::GitWorkspace;
use codex_infra_state::RecordedCheckpoint;

use crate::AgentContext;
use crate::WorkspaceGate;
use crate::WorkspaceLease;

struct CheckpointState {
    workspace: GitWorkspace,
    coordinator: CheckpointCoordinator,
}

/// Serializes one Agent's commit/push work off the async runtime. The service
/// retains its mutation boundary until foreground/background lease shares have
/// been released and checkpoint publication has completed.
/// This service does not interpret a yielded tool call as a process exit.
#[derive(Clone)]
pub struct WorkspaceCheckpoints {
    state: Arc<Mutex<CheckpointState>>,
    context: Arc<AgentContext>,
    gate: WorkspaceGate,
}

impl WorkspaceCheckpoints {
    /// Uses an already resumed workspace and recovered journal. The last
    /// completed push is restored even when a newer operation is pending.
    pub fn new(
        workspace: GitWorkspace,
        coordinator: CheckpointCoordinator,
        context: Arc<AgentContext>,
    ) -> io::Result<Self> {
        context.validate_workspace(workspace.binding())?;
        if let Some(record) = coordinator.completed(workspace.binding().agent_id) {
            context.apply_checkpoint(record)?;
        }
        Ok(Self {
            state: Arc::new(Mutex::new(CheckpointState {
                workspace,
                coordinator,
            })),
            context,
            gate: WorkspaceGate::new(),
        })
    }

    pub fn gate(&self) -> WorkspaceGate {
        self.gate.clone()
    }

    /// Returns only after commit, push confirmation, journal completion, and
    /// publication of the new model binding. Cancellation of the waiter does
    /// not cancel an in-flight Git operation or its binding update.
    pub async fn checkpoint(
        &self,
        lease: WorkspaceLease,
        kind: CheckpointKind,
    ) -> io::Result<RecordedCheckpoint> {
        let state = Arc::clone(&self.state);
        let context = Arc::clone(&self.context);
        self.gate
            .run(lease, move |operation_id| {
                let mut state = state
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                let CheckpointState {
                    workspace,
                    coordinator,
                } = &mut *state;
                coordinator.checkpoint(workspace, operation_id, kind)?;
                let record = coordinator
                    .completed(workspace.binding().agent_id)
                    .ok_or_else(|| io::Error::other("completed checkpoint record missing"))?
                    .clone();
                context.apply_checkpoint(&record)?;
                Ok(record)
            })
            .await
    }

    /// Reconciles a pending operation before admitting new work. The original
    /// operation ID/kind is reused, including after a successful push whose
    /// completion record was interrupted. No new finalization is invented.
    pub async fn recover(&self, lease: WorkspaceLease) -> io::Result<Option<RecordedCheckpoint>> {
        let state = Arc::clone(&self.state);
        let context = Arc::clone(&self.context);
        self.gate
            .run(lease, move |_operation_id| {
                let mut state = state
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                let CheckpointState {
                    workspace,
                    coordinator,
                } = &mut *state;
                let agent_id = workspace.binding().agent_id;
                if let Some(CheckpointPhase::Pending { attempt }) = coordinator.phase(agent_id) {
                    let operation_id = attempt.operation_id.clone();
                    let kind = attempt.kind;
                    coordinator.checkpoint(workspace, &operation_id, kind)?;
                }
                let record = coordinator.completed(agent_id).cloned();
                if let Some(record) = &record {
                    context.apply_checkpoint(record)?;
                }
                Ok(record)
            })
            .await
    }
}
