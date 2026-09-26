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

/// Journals pending before Git work and completed only after push confirmation.
/// The machine writer owns this coordinator; every Agent's latest phase is
/// reconstructed before result delivery or finalization can resume.
pub struct CheckpointCoordinator {
    journal: Journal,
    phases: BTreeMap<AgentId, CheckpointPhase>,
}

impl CheckpointCoordinator {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut phases = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            let phase: CheckpointPhase =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            let agent_id = match &phase {
                CheckpointPhase::Pending { attempt }
                | CheckpointPhase::Completed { attempt, .. } => attempt.workspace.agent_id,
            };
            phases.insert(agent_id, phase);
            Ok(())
        })?;
        Ok(Self { journal, phases })
    }

    pub fn phase(&self, agent_id: AgentId) -> Option<&CheckpointPhase> {
        self.phases.get(&agent_id)
    }

    pub fn checkpoint(
        &mut self,
        workspace: &mut GitWorkspace,
        operation_id: &str,
        kind: CheckpointKind,
    ) -> io::Result<Checkpoint> {
        let agent_id = workspace.binding().agent_id;
        let attempt = match self.phases.get(&agent_id) {
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
        self.journal
            .append(&serde_json::to_vec(&pending).map_err(io::Error::other)?)?;
        self.phases.insert(agent_id, pending);
        let mut receipt = workspace.checkpoint(operation_id)?;
        receipt.before = attempt.before.clone();
        let completed = CheckpointPhase::Completed {
            attempt,
            receipt: receipt.clone(),
        };
        self.journal
            .append(&serde_json::to_vec(&completed).map_err(io::Error::other)?)?;
        self.phases.insert(agent_id, completed);
        Ok(receipt)
    }
}
