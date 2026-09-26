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
use serde_json::Value;
use tokio::sync::watch;

/// Host-owned execution bindings observed atomically at each sampling step.
/// Runtime updates this from its verified launch/checkpoint/generation state.
/// Tasks, peer messages and directory listings enter through their own inputs.
pub struct AgentContext {
    current: watch::Sender<[AgentExecutionFragment; 3]>,
}

impl AgentContext {
    pub fn new(
        identity: ExecutionIdentity,
        workspace: ExecutionWorkspace,
        inference: ExecutionInference,
    ) -> io::Result<Self> {
        let fragments = fragments(identity, workspace, inference)?;
        let (current, _) = watch::channel(fragments);
        Ok(Self { current })
    }

    /// Validates the complete next snapshot before publishing any section.
    pub fn replace(
        &self,
        identity: ExecutionIdentity,
        workspace: ExecutionWorkspace,
        inference: ExecutionInference,
    ) -> io::Result<()> {
        self.current
            .send_replace(fragments(identity, workspace, inference)?);
        Ok(())
    }
}

impl ContextContributor for AgentContext {
    fn contribute_world_state<'a>(
        &'a self,
        _input: WorldStateContributionInput<'a>,
    ) -> ExtensionFuture<'a, Vec<WorldStateSectionContribution>> {
        Box::pin(async move {
            let current = self.current.borrow().clone();
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

fn fragments(
    identity: ExecutionIdentity,
    workspace: ExecutionWorkspace,
    inference: ExecutionInference,
) -> io::Result<[AgentExecutionFragment; 3]> {
    Ok([
        AgentExecutionFragment::new(AgentExecutionContext::Identity(identity))?,
        AgentExecutionFragment::new(AgentExecutionContext::Workspace(workspace))?,
        AgentExecutionFragment::new(AgentExecutionContext::Inference(inference))?,
    ])
}
