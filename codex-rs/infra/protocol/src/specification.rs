use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::AgentId;
use crate::CommitId;
use crate::Dependency;
use crate::MachineId;
use crate::TaskId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BuildIntent {
    NoBuild,
    Derivation { attribute: String },
}

/// The Agent's declared work, recorded before Nix resolution and execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub task_id: TaskId,
    pub objective: String,
    pub scope: String,
    pub owner_machine_id: MachineId,
    pub revision: u64,
    pub repo: String,
    pub source_commit: CommitId,
    pub flake_reference: String,
    pub target_machine: MachineId,
    pub nix_system: String,
    pub build: BuildIntent,
    pub runtime_command: Vec<String>,
    pub dependencies: Vec<Dependency>,
    pub expected_outputs: Vec<String>,
    pub assigned_agent: AgentId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceBinding {
    pub provider_id: String,
    pub account_id: String,
    pub model_id: String,
    pub credential_revision: CommitId,
    pub selection_reason: String,
}

/// Native store paths refer to the generation's target Nix system. Token values
/// live in the referenced Team State commit and realized generation files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigGeneration {
    pub codex_source_repository: String,
    pub codex_source_commit: CommitId,
    pub team_state_repository: String,
    pub team_state_commit: CommitId,
    pub config_ref: String,
    pub config_commit: CommitId,
    pub source_flake_lock_hash: String,
    pub state_flake_lock_hash: String,
    pub nix_system: String,
    pub config_derivation: PathBuf,
    pub config_store_path: PathBuf,
    pub effective_config_digest: String,
    pub inference: InferenceBinding,
    pub role_registry_revision: CommitId,
    pub cooperation_protocol_revision: CommitId,
    pub skills_revision: CommitId,
    pub memory_revision: CommitId,
}
