use std::io;

use codex_core::context::ExecutionIdentity;
use codex_core::context::ExecutionInference;
use codex_core::context::ExecutionWorkspace;
use codex_infra_protocol::AgentId;
use codex_infra_protocol::TaskSpec;
use codex_infra_runtime::LaunchIntent;

use crate::AgentContext;

impl AgentContext {
    /// Builds the initial model view from the machine runtime's launch binding
    /// and the Task accepted through the Agent's pane. Parent is task ancestry,
    /// independent of the host process lifetime.
    pub fn for_launch(
        launch: &LaunchIntent,
        task: &TaskSpec,
        parent_agent_id: Option<AgentId>,
    ) -> io::Result<Self> {
        if task.task_id != launch.task_id
            || task.assigned_agent != launch.workspace.agent_id
            || task.target_machine != launch.machine_id
        {
            return Err(io::Error::other("Task does not match Agent launch binding"));
        }
        let generation = &launch.generation;
        let inference = &generation.inference;
        let worktree = launch.workspace.worktree.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Agent worktree path is not UTF-8",
            )
        })?;
        let config_generation = generation.config_store_path.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "configuration store path is not UTF-8",
            )
        })?;
        Self::new(
            ExecutionIdentity {
                agent_id: launch.workspace.agent_id.to_string(),
                root_session_id: launch.workspace.root_session_id.to_string(),
                parent_agent_id: parent_agent_id.map(|id| id.to_string()),
                machine_id: launch.machine_id.to_string(),
                task_id: launch.task_id.to_string(),
                role: launch.role.clone(),
            },
            ExecutionWorkspace {
                repo: task.repo.clone(),
                worktree: worktree.to_owned(),
                branch: launch.workspace.branch.clone(),
                commit: launch.initial_commit.to_string(),
            },
            ExecutionInference {
                provider: inference.provider_id.clone(),
                account: inference.account_id.clone(),
                model_id: inference.model_id.clone(),
                credential_revision: inference.credential_revision.to_string(),
                config_generation: config_generation.to_owned(),
                role_revision: generation.role_registry_revision.to_string(),
                skills_revision: generation.skills_revision.to_string(),
                memory_revision: generation.memory_revision.to_string(),
            },
        )
    }
}
