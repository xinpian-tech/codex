use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::CommitId;
use serde::Deserialize;
use serde::Serialize;

use crate::Checkpoint;
use crate::GitWorkspace;
use crate::Journal;
use crate::WorkspaceBinding;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointKind {
    Mutation,
    Finalization,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointAttempt {
    pub workspace: WorkspaceBinding,
    pub operation_id: String,
    pub kind: CheckpointKind,
    pub before: CommitId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum CheckpointPhase {
    Pending {
        attempt: CheckpointAttempt,
    },
    Completed {
        attempt: CheckpointAttempt,
        receipt: Checkpoint,
    },
}

/// The phase's durable position in the owning machine's checkpoint journal.
#[derive(Debug, Clone)]
pub struct RecordedCheckpoint {
    pub sequence: u64,
    pub phase: CheckpointPhase,
}

/// Journals pending before Git work and completed only after push confirmation.
/// The machine writer owns this coordinator; every Agent's latest phase is
/// reconstructed before result delivery or finalization can resume.
pub struct CheckpointCoordinator {
    journal: Journal,
    phases: BTreeMap<AgentId, RecordedCheckpoint>,
    completed: BTreeMap<AgentId, RecordedCheckpoint>,
}

impl CheckpointCoordinator {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut phases = BTreeMap::new();
        let mut completed = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            let phase: CheckpointPhase =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            let agent_id = match &phase {
                CheckpointPhase::Pending { attempt }
                | CheckpointPhase::Completed { attempt, .. } => attempt.workspace.agent_id,
            };
            let record = RecordedCheckpoint {
                sequence: record.sequence,
                phase,
            };
            if matches!(record.phase, CheckpointPhase::Completed { .. }) {
                completed.insert(agent_id, record.clone());
            }
            phases.insert(agent_id, record);
            Ok(())
        })?;
        Ok(Self {
            journal,
            phases,
            completed,
        })
    }

    pub fn phase(&self, agent_id: AgentId) -> Option<&CheckpointPhase> {
        self.phases.get(&agent_id).map(|record| &record.phase)
    }

    pub fn record(&self, agent_id: AgentId) -> Option<&RecordedCheckpoint> {
        self.phases.get(&agent_id)
    }

    /// The last pushed result remains available while a later mutation is pending.
    pub fn completed(&self, agent_id: AgentId) -> Option<&RecordedCheckpoint> {
        self.completed.get(&agent_id)
    }

    pub fn checkpoint(
        &mut self,
        workspace: &mut GitWorkspace,
        operation_id: &str,
        kind: CheckpointKind,
    ) -> io::Result<Checkpoint> {
        let agent_id = workspace.binding().agent_id;
        if let Some(phase) = self.phase(agent_id) {
            let attempt = match phase {
                CheckpointPhase::Pending { attempt }
                | CheckpointPhase::Completed { attempt, .. } => attempt,
            };
            if &attempt.workspace != workspace.binding() {
                return Err(io::Error::other("checkpoint workspace binding changed"));
            }
        }
        let attempt = match self.phase(agent_id) {
            Some(CheckpointPhase::Completed { attempt, receipt })
                if attempt.operation_id == operation_id && attempt.kind == kind =>
            {
                return Ok(receipt.clone());
            }
            Some(CheckpointPhase::Pending { attempt }) => {
                if attempt.operation_id != operation_id || attempt.kind != kind {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!("checkpoint {} is pending", attempt.operation_id),
                    ));
                }
                attempt.clone()
            }
            Some(CheckpointPhase::Completed { .. }) | None => CheckpointAttempt {
                workspace: workspace.binding().clone(),
                operation_id: operation_id.to_owned(),
                kind,
                before: workspace.current_commit()?,
            },
        };
        let pending = CheckpointPhase::Pending {
            attempt: attempt.clone(),
        };
        let sequence = self
            .journal
            .append(&serde_json::to_vec(&pending).map_err(io::Error::other)?)?;
        self.phases.insert(
            agent_id,
            RecordedCheckpoint {
                sequence,
                phase: pending,
            },
        );
        let mut receipt = workspace.checkpoint(operation_id)?;
        receipt.before = attempt.before.clone();
        let completed = CheckpointPhase::Completed {
            attempt,
            receipt: receipt.clone(),
        };
        let sequence = self
            .journal
            .append(&serde_json::to_vec(&completed).map_err(io::Error::other)?)?;
        let record = RecordedCheckpoint {
            sequence,
            phase: completed,
        };
        self.completed.insert(agent_id, record.clone());
        self.phases.insert(agent_id, record);
        Ok(receipt)
    }
}
