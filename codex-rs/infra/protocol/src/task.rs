use serde::Deserialize;
use serde::Serialize;

use crate::AgentId;
use crate::AssignmentId;
use crate::CommitId;
use crate::ContributionId;
use crate::MachineId;
use crate::TaskId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Dependency {
    TaskOutput { task_id: TaskId },
    IntegratedContribution { contribution_id: ContributionId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Waiting,
    Ready,
    Assigned,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AssignmentStatus {
    Active,
    HandedOff { commit: CommitId },
    Completed { commit: CommitId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub assignment_id: AssignmentId,
    pub agent_id: AgentId,
    pub status: AssignmentStatus,
}

/// A task owner applies transitions serially and persists each new revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    task_id: TaskId,
    owner_machine_id: MachineId,
    revision: u64,
    pending_dependencies: Vec<Dependency>,
    status: TaskStatus,
    assignments: Vec<Assignment>,
}

#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("task is owned by machine {0}")]
    Owner(MachineId),
    #[error("task revision changed: expected {expected}, actual {actual}")]
    Revision { expected: u64, actual: u64 },
    #[error("task is {0:?}")]
    Status(TaskStatus),
    #[error("assignment {0} is not the current active assignment")]
    Assignment(AssignmentId),
    #[error("assignment ID was already used: {0}")]
    DuplicateAssignment(AssignmentId),
    #[error("dependency is not pending")]
    Dependency,
    #[error("task revision exhausted")]
    RevisionExhausted,
}

impl TaskRecord {
    pub fn new(task_id: TaskId, owner: MachineId, dependencies: Vec<Dependency>) -> Self {
        let status = if dependencies.is_empty() {
            TaskStatus::Ready
        } else {
            TaskStatus::Waiting
        };
        Self {
            task_id,
            owner_machine_id: owner,
            revision: 0,
            pending_dependencies: dependencies,
            status,
            assignments: Vec::new(),
        }
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn owner(&self) -> &MachineId {
        &self.owner_machine_id
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn status(&self) -> TaskStatus {
        self.status
    }

    pub fn assignments(&self) -> &[Assignment] {
        &self.assignments
    }

    pub fn pending_dependencies(&self) -> &[Dependency] {
        &self.pending_dependencies
    }

    pub fn claim(
        &mut self,
        owner: &MachineId,
        revision: u64,
        assignment_id: AssignmentId,
        agent_id: AgentId,
    ) -> Result<(), TaskError> {
        self.check_revision(owner, revision)?;
        if self.status != TaskStatus::Ready {
            return Err(TaskError::Status(self.status));
        }
        if self
            .assignments
            .iter()
            .any(|item| item.assignment_id == assignment_id)
        {
            return Err(TaskError::DuplicateAssignment(assignment_id));
        }
        self.assignments.push(Assignment {
            assignment_id,
            agent_id,
            status: AssignmentStatus::Active,
        });
        self.status = TaskStatus::Assigned;
        self.revision += 1;
        Ok(())
    }

    pub fn satisfy_dependency(
        &mut self,
        owner: &MachineId,
        revision: u64,
        dependency: &Dependency,
    ) -> Result<(), TaskError> {
        self.check_revision(owner, revision)?;
        if self.status != TaskStatus::Waiting {
            return Err(TaskError::Status(self.status));
        }
        if !self.pending_dependencies.contains(dependency) {
            return Err(TaskError::Dependency);
        }
        self.pending_dependencies.retain(|item| item != dependency);
        if self.pending_dependencies.is_empty() {
            self.status = TaskStatus::Ready;
        }
        self.revision += 1;
        Ok(())
    }

    pub fn handoff(
        &mut self,
        owner: &MachineId,
        revision: u64,
        assignment_id: AssignmentId,
        commit: CommitId,
    ) -> Result<(), TaskError> {
        self.finish_assignment(
            owner,
            revision,
            assignment_id,
            AssignmentStatus::HandedOff { commit },
        )?;
        self.status = TaskStatus::Ready;
        Ok(())
    }

    pub fn complete(
        &mut self,
        owner: &MachineId,
        revision: u64,
        assignment_id: AssignmentId,
        commit: CommitId,
    ) -> Result<(), TaskError> {
        self.finish_assignment(
            owner,
            revision,
            assignment_id,
            AssignmentStatus::Completed { commit },
        )?;
        self.status = TaskStatus::Completed;
        Ok(())
    }

    fn finish_assignment(
        &mut self,
        owner: &MachineId,
        revision: u64,
        assignment_id: AssignmentId,
        status: AssignmentStatus,
    ) -> Result<(), TaskError> {
        self.check_revision(owner, revision)?;
        if self.status != TaskStatus::Assigned {
            return Err(TaskError::Status(self.status));
        }
        let assignment = self
            .assignments
            .last_mut()
            .filter(|item| {
                item.assignment_id == assignment_id && item.status == AssignmentStatus::Active
            })
            .ok_or(TaskError::Assignment(assignment_id))?;
        assignment.status = status;
        self.revision += 1;
        Ok(())
    }

    fn check_revision(&self, owner: &MachineId, revision: u64) -> Result<(), TaskError> {
        if owner != &self.owner_machine_id {
            return Err(TaskError::Owner(self.owner_machine_id.clone()));
        }
        if revision != self.revision {
            return Err(TaskError::Revision {
                expected: revision,
                actual: self.revision,
            });
        }
        if self.revision == u64::MAX {
            return Err(TaskError::RevisionExhausted);
        }
        Ok(())
    }
}
