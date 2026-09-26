use std::io;

use codex_core::context::AgentExecutionContext;
use codex_core::context::AgentExecutionFragment;
use codex_core::context::ContextualUserFragment;
use codex_core::context::ExecutionIdentity;
use codex_core::context::ExecutionInference;
use codex_core::context::ExecutionWorkspace;
use codex_extension_api::ContextContributor;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::PreviousWorldStateSection;
use codex_extension_api::RenderedWorldStateFragment;
use codex_extension_api::WorldStateContributionInput;
use codex_extension_api::WorldStateSectionContribution;
use codex_infra_state::CheckpointPhase;
use codex_infra_state::RecordedCheckpoint;
use codex_infra_state::WorkspaceBinding;
use serde_json::Value;
use tokio::sync::watch;

/// Host-owned execution bindings observed atomically at each sampling step.
/// Runtime updates this from its verified launch/checkpoint/generation state.
/// Tasks, peer messages and directory listings enter through their own inputs.
pub struct AgentContext {
    current: watch::Sender<ContextSnapshot>,
}

#[derive(Clone)]
struct ContextSnapshot {
    identity: ExecutionIdentity,
    workspace: ExecutionWorkspace,
    inference: ExecutionInference,
    fragments: [AgentExecutionFragment; 3],
    checkpoint_sequence: Option<u64>,
}

impl ContextSnapshot {
    fn matches_workspace(&self, workspace: &WorkspaceBinding) -> bool {
        workspace.agent_id.to_string() == self.identity.agent_id
            && workspace.root_session_id.to_string() == self.identity.root_session_id
            && workspace.worktree.to_str() == Some(self.workspace.worktree.as_str())
            && workspace.branch == self.workspace.branch
    }
}

impl AgentContext {
    pub(crate) fn validate_message_source(
        &self,
        message: &codex_infra_protocol::AgentMessage,
    ) -> io::Result<()> {
        let current = self.current.borrow();
        if message.from.agent_id.to_string() != current.identity.agent_id
            || message.root_session_id.to_string() != current.identity.root_session_id
            || message.from.machine_id.to_string() != current.identity.machine_id
            || message.from.role != current.identity.role
            || message.repo != current.workspace.repo
        {
            return Err(io::Error::other(
                "message source does not match Agent binding",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_workspace(&self, workspace: &WorkspaceBinding) -> io::Result<()> {
        if !self.current.borrow().matches_workspace(workspace) {
            return Err(io::Error::other("workspace does not match Agent context"));
        }
        Ok(())
    }

    pub(crate) fn new(
        identity: ExecutionIdentity,
        workspace: ExecutionWorkspace,
        inference: ExecutionInference,
    ) -> io::Result<Self> {
        let snapshot = snapshot(identity, workspace, inference)?;
        let (current, _) = watch::channel(snapshot);
        Ok(Self { current })
    }

    /// Applies an already completed commit/push hook. Caller replays completed
    /// checkpoints after opening a resumed host and before its first sampling.
    pub fn apply_checkpoint(&self, record: &RecordedCheckpoint) -> io::Result<()> {
        let CheckpointPhase::Completed { attempt, receipt } = &record.phase else {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "checkpoint is still pending",
            ));
        };
        let mut result = Ok(());
        self.current.send_if_modified(|current| {
            if !current.matches_workspace(&attempt.workspace)
                || attempt.operation_id != receipt.operation_id
                || attempt.before != receipt.before
            {
                result = Err(io::Error::other(
                    "checkpoint does not match Agent workspace",
                ));
                return false;
            }
            if let Some(previous) = current.checkpoint_sequence {
                if record.sequence == previous
                    && current.workspace.commit == receipt.pushed_commit.to_string()
                {
                    return false;
                }
                if record.sequence <= previous {
                    result = Err(io::Error::other(
                        "checkpoint is older than current Agent binding",
                    ));
                    return false;
                }
            }
            let mut workspace = current.workspace.clone();
            workspace.commit = receipt.pushed_commit.to_string();
            match snapshot(
                current.identity.clone(),
                workspace,
                current.inference.clone(),
            ) {
                Ok(mut next) => {
                    next.checkpoint_sequence = Some(record.sequence);
                    *current = next;
                    true
                }
                Err(error) => {
                    result = Err(error);
                    false
                }
            }
        });
        result
    }
}

impl ContextContributor for AgentContext {
    fn contribute_world_state<'a>(
        &'a self,
        _input: WorldStateContributionInput<'a>,
    ) -> ExtensionFuture<'a, Vec<WorldStateSectionContribution>> {
        Box::pin(async move {
            let current = self.current.borrow().fragments.clone();
            ["infra_identity", "infra_workspace", "infra_inference"]
                .into_iter()
                .zip(current)
                .map(|(id, fragment)| {
                    let snapshot = Value::String(fragment.body());
                    let comparison = snapshot.clone();
                    let retained = fragment.render();
                    WorldStateSectionContribution::new(id, snapshot, move |previous| {
                        if let PreviousWorldStateSection::Known(value) = previous
                            && value == &comparison
                        {
                            return None;
                        }
                        Some(RenderedWorldStateFragment::new(
                            fragment.role(),
                            fragment.markers(),
                            fragment.body(),
                        ))
                    })
                    .with_retained_fragment_matcher(move |role, text| {
                        role == "user" && text.contains(&retained)
                    })
                })
                .collect()
        })
    }
}

fn snapshot(
    identity: ExecutionIdentity,
    workspace: ExecutionWorkspace,
    inference: ExecutionInference,
) -> io::Result<ContextSnapshot> {
    let fragments = [
        AgentExecutionFragment::new(AgentExecutionContext::Identity(identity.clone()))?,
        AgentExecutionFragment::new(AgentExecutionContext::Workspace(workspace.clone()))?,
        AgentExecutionFragment::new(AgentExecutionContext::Inference(inference.clone()))?,
    ];
    Ok(ContextSnapshot {
        identity,
        workspace,
        inference,
        fragments,
        checkpoint_sequence: None,
    })
}
