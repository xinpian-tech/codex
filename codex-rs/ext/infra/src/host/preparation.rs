use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageKind;
use codex_infra_protocol::TaskSpec;
use codex_infra_runtime::LaunchIntent;
use codex_infra_state::CheckpointCoordinator;
use codex_infra_state::CheckpointPhase;
use codex_infra_state::GitWorkspace;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;

use super::AgentHostGeneration;
use super::ManagedHostServices;
use crate::AgentContext;
use crate::ProcessAudit;
use crate::StoreAudit;
use crate::StoreAuditIdentity;
use crate::ToolAudit;
use crate::ToolWorkspace;
use crate::WorkspaceCheckpoints;

/// Host-local programs and persistent spool supplied by the deployed generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPreparationConfig {
    pub hostid: PathBuf,
    pub git: PathBuf,
    pub spool_directory: PathBuf,
}

/// Resources prepared before inference starts. The caller handles pending
/// checkpoint recovery before admitting work, then starts the managed services.
pub struct PreparedAgentHost {
    pub launch: LaunchIntent,
    pub services: ManagedHostServices,
    pub processes: ProcessAudit,
    pub checkpoints: WorkspaceCheckpoints,
    pub context: Arc<AgentContext>,
    pub task: TaskSpec,
    pub directory: PathBuf,
    pub home: PathBuf,
    pub generation: Option<AgentHostGeneration>,
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
struct PreparationBinding {
    config: AgentPreparationConfig,
    launch: LaunchIntent,
    bootstrap: AgentMessage,
    #[serde(default)]
    generation: Option<AgentHostGeneration>,
}

impl PreparedAgentHost {
    /// Loads and records immutable host configuration before opening resources.
    /// The bootstrap still comes from the host's own recorded terminal input.
    pub async fn open_generation(
        launch: LaunchIntent,
        bootstrap: AgentMessage,
    ) -> io::Result<Self> {
        let binding = tokio::task::spawn_blocking(move || {
            let generation = AgentHostGeneration::read(&launch.generation)?;
            Ok::<_, io::Error>(PreparationBinding {
                config: generation.config.preparation.clone(),
                launch,
                bootstrap,
                generation: Some(generation),
            })
        })
        .await
        .map_err(io::Error::other)??;
        Self::open_binding(binding).await
    }

    /// Consumes the bootstrap envelope already recorded by HostMailbox from
    /// tmux stdin. Its body is the assigned TaskSpec; the envelope's repository
    /// and commit describe the sender, which may work in another repository.
    /// File recovery and Git/hostid commands run outside the async executor.
    pub async fn open(
        config: AgentPreparationConfig,
        launch: LaunchIntent,
        bootstrap: AgentMessage,
    ) -> io::Result<Self> {
        Self::open_binding(PreparationBinding {
            config,
            launch,
            bootstrap,
            generation: None,
        })
        .await
    }

    async fn open_binding(binding: PreparationBinding) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let launch = &binding.launch;
            let bootstrap = &binding.bootstrap;
            if bootstrap.kind != MessageKind::Bootstrap
                || bootstrap.root_session_id != launch.workspace.root_session_id
                || bootstrap.task_id != launch.task_id
                || bootstrap.to.agent_id != launch.workspace.agent_id
                || bootstrap.to.machine_id != launch.machine_id
                || bootstrap.to.role != launch.role
            {
                return Err(io::Error::other("bootstrap does not match Agent launch"));
            }
            let task: TaskSpec = serde_json::from_str(&bootstrap.body)?;
            let context = Arc::new(AgentContext::for_launch(
                launch,
                &task,
                Some(bootstrap.from.agent_id),
            )?);
            let output = Command::new(&binding.config.hostid).output()?;
            if !output.status.success() {
                return Err(io::Error::other(
                    String::from_utf8_lossy(&output.stderr).into_owned(),
                ));
            }
            let machine_id: MachineId = std::str::from_utf8(&output.stdout)
                .map_err(io::Error::other)?
                .trim()
                .parse()
                .map_err(io::Error::other)?;
            if machine_id != launch.machine_id {
                return Err(io::Error::other("Agent launch belongs to another hostid"));
            }
            let agent_directory = binding
                .config
                .spool_directory
                .join(launch.workspace.root_session_id.to_string())
                .join(launch.workspace.agent_id.to_string());
            let directory = agent_directory.join(launch.launch_id.to_string());
            std::fs::create_dir_all(&directory)?;
            let directory = directory.canonicalize()?;
            let mut recorded = false;
            let mut journal = Journal::open(&directory.join("preparation.journal"), |record| {
                let previous: PreparationBinding = serde_json::from_slice(&record.payload)?;
                if previous != binding {
                    return Err(io::Error::other("Agent preparation binding changed"));
                }
                recorded = true;
                Ok(())
            })?;
            if !recorded {
                journal.append(&serde_json::to_vec(&binding)?)?;
            }
            let home = match &binding.generation {
                Some(generation) => {
                    super::home::prepare_home(&directory, &launch.generation, generation)?
                }
                None => {
                    let home = directory.join("home");
                    std::fs::create_dir_all(&home)?;
                    home
                }
            };
            // Checkpoint history follows the Agent across host launches. The
            // coordinator's exclusive writer also serializes worktree owners.
            let coordinator =
                CheckpointCoordinator::open(&agent_directory.join("checkpoints.journal"))?;
            let workspace =
                GitWorkspace::resume(binding.config.git.clone(), launch.workspace.clone())?;
            let expected_commit = match coordinator.phase(launch.workspace.agent_id) {
                Some(CheckpointPhase::Pending { attempt }) => {
                    if attempt.workspace != launch.workspace {
                        return Err(io::Error::other("pending checkpoint workspace changed"));
                    }
                    None
                }
                Some(CheckpointPhase::Completed { attempt, receipt }) => {
                    if attempt.workspace != launch.workspace {
                        return Err(io::Error::other("completed checkpoint workspace changed"));
                    }
                    Some(&receipt.pushed_commit)
                }
                None => Some(&launch.initial_commit),
            };
            if let Some(expected) = expected_commit
                && workspace.current_commit()? != *expected
            {
                return Err(io::Error::other(
                    "worktree HEAD differs from recorded commit",
                ));
            }
            let checkpoints =
                WorkspaceCheckpoints::new(workspace, coordinator, Arc::clone(&context))?;
            let identity = StoreAuditIdentity {
                root_session_id: launch.workspace.root_session_id,
                agent_id: launch.workspace.agent_id,
                machine_id,
            };
            let audit =
                StoreAudit::open(&directory.join("thread-store.journal"), identity.clone())?;
            let tools = ToolAudit::open(
                &directory.join("tools.journal"),
                identity.clone(),
                launch.launch_id,
            )?
            .with_workspace(ToolWorkspace::new(checkpoints.clone()))?;
            let processes = ProcessAudit::open(
                &directory.join("processes.journal"),
                identity,
                launch.launch_id,
            )?;
            let mut services =
                ManagedHostServices::new(audit, Arc::clone(&context), Arc::new(tools));
            services.launch_binding = Some(Arc::new(journal));
            services.home = Some(home.clone());
            Ok(Self {
                launch: binding.launch,
                services,
                processes,
                checkpoints,
                context,
                task,
                directory,
                home,
                generation: binding.generation,
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}
