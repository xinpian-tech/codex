use serde::Deserialize;
use serde::Serialize;

use crate::AgentId;
use crate::AssignmentId;
use crate::CommitId;
use crate::ContributionId;
use crate::TaskId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContributionStatus {
    Ready,
    Integrated { integration_commit: CommitId },
}

/// An author's published unit of work, distinct from individual checkpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Contribution {
    #[serde(flatten)]
    pub proposal: ContributionProposal,
    status: ContributionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContributionProposal {
    pub contribution_id: ContributionId,
    pub task_id: TaskId,
    pub assignment_id: AssignmentId,
    pub author_agent_id: AgentId,
    pub repo: String,
    pub base_commit: CommitId,
    pub head_commit: CommitId,
    pub dependencies: Vec<ContributionId>,
    pub target_branch: String,
    pub integration_owner: AgentId,
}

#[derive(Debug, thiserror::Error)]
pub enum ContributionError {
    #[error("contribution integration is assigned to {0}")]
    Owner(AgentId),
    #[error("contribution already integrated at {0}")]
    Integrated(CommitId),
}

impl Contribution {
    pub fn ready(proposal: ContributionProposal) -> Self {
        Self {
            proposal,
            status: ContributionStatus::Ready,
        }
    }

    pub fn status(&self) -> &ContributionStatus {
        &self.status
    }

    /// Called after the integration owner's target branch push is confirmed.
    pub fn integrate(
        &mut self,
        agent_id: AgentId,
        integration_commit: CommitId,
    ) -> Result<(), ContributionError> {
        if agent_id != self.proposal.integration_owner {
            return Err(ContributionError::Owner(self.proposal.integration_owner));
        }
        match &self.status {
            ContributionStatus::Ready => {
                self.status = ContributionStatus::Integrated { integration_commit };
                Ok(())
            }
            ContributionStatus::Integrated {
                integration_commit: existing,
            } if existing == &integration_commit => Ok(()),
            ContributionStatus::Integrated { integration_commit } => {
                Err(ContributionError::Integrated(integration_commit.clone()))
            }
        }
    }
}
