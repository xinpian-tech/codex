use std::io;

use codex_infra_protocol::*;
use codex_infra_state::TaskStore;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum TaskCommand {
    Create {
        record: TaskRecord,
    },
    Get {
        task_id: TaskId,
    },
    List {
        after: Option<TaskId>,
    },
    Claim {
        task_id: TaskId,
        revision: u64,
        assignment_id: AssignmentId,
        agent_id: AgentId,
    },
    Complete {
        task_id: TaskId,
        assignment_id: AssignmentId,
        agent_id: AgentId,
        commit: CommitId,
    },
    Handoff {
        task_id: TaskId,
        assignment_id: AssignmentId,
        agent_id: AgentId,
        commit: CommitId,
    },
    Integrated {
        contribution_id: ContributionId,
    },
}

pub(super) fn apply(
    store: &mut TaskStore,
    owner: &MachineId,
    command: TaskCommand,
) -> io::Result<Vec<TaskRecord>> {
    let task_id = match command {
        TaskCommand::Create { record } => {
            if record.owner() != owner {
                return Err(io::Error::other("create Task on its owning machine"));
            }
            let id = record.task_id();
            let satisfied: Vec<_> = record
                .pending_dependencies()
                .iter()
                .filter(|dependency| match dependency {
                    Dependency::TaskOutput { task_id } => store
                        .get(*task_id)
                        .is_some_and(|task| task.status() == TaskStatus::Completed),
                    Dependency::IntegratedContribution { .. } => false,
                })
                .cloned()
                .collect();
            store.create(record).map_err(io::Error::other)?;
            for dependency in satisfied {
                satisfy(store, owner, dependency)?;
            }
            id
        }
        TaskCommand::Get { task_id } => task_id,
        TaskCommand::List { after } => {
            return Ok(store
                .records()
                .filter(|task| after.is_none_or(|id| task.task_id() > id))
                .take(32)
                .cloned()
                .collect());
        }
        TaskCommand::Claim {
            task_id,
            revision,
            assignment_id,
            agent_id,
        } => {
            store
                .update(task_id, |task| {
                    task.claim(owner, revision, assignment_id, agent_id)
                })
                .map_err(io::Error::other)?;
            task_id
        }
        TaskCommand::Integrated { contribution_id } => {
            return satisfy(
                store,
                owner,
                Dependency::IntegratedContribution { contribution_id },
            );
        }
        command @ (TaskCommand::Complete { .. } | TaskCommand::Handoff { .. }) => {
            let (task_id, assignment_id, agent_id, commit, complete) = match command {
                TaskCommand::Complete {
                    task_id,
                    assignment_id,
                    agent_id,
                    commit,
                } => (task_id, assignment_id, agent_id, commit, true),
                TaskCommand::Handoff {
                    task_id,
                    assignment_id,
                    agent_id,
                    commit,
                } => (task_id, assignment_id, agent_id, commit, false),
                TaskCommand::Create { .. }
                | TaskCommand::Get { .. }
                | TaskCommand::List { .. }
                | TaskCommand::Claim { .. }
                | TaskCommand::Integrated { .. } => {
                    return Err(io::Error::other("expected assignment result"));
                }
            };
            let record = store
                .get(task_id)
                .ok_or_else(|| io::Error::other("unknown Task"))?;
            let assignment = record
                .assignments()
                .last()
                .ok_or_else(|| io::Error::other("Task has no assignment"))?;
            if assignment.assignment_id != assignment_id || assignment.agent_id != agent_id {
                return Err(io::Error::other("result differs from assigned Agent"));
            }
            if assignment.status != AssignmentStatus::Active {
                return Ok(vec![record.clone()]);
            }
            let revision = record.revision();
            store
                .update(task_id, |task| {
                    if complete {
                        task.complete(owner, revision, assignment_id, commit)
                    } else {
                        task.handoff(owner, revision, assignment_id, commit)
                    }
                })
                .map_err(io::Error::other)?;
            if complete {
                satisfy(store, owner, Dependency::TaskOutput { task_id })?;
            }
            task_id
        }
    };
    Ok(store.get(task_id).cloned().into_iter().collect())
}

fn satisfy(
    store: &mut TaskStore,
    owner: &MachineId,
    dependency: Dependency,
) -> io::Result<Vec<TaskRecord>> {
    let waiting: Vec<_> = store
        .records()
        .filter(|task| task.pending_dependencies().contains(&dependency))
        .map(|task| (task.task_id(), task.revision()))
        .collect();
    let mut updated = Vec::new();
    for (id, revision) in waiting {
        store
            .update(id, |task| {
                task.satisfy_dependency(owner, revision, &dependency)
            })
            .map_err(io::Error::other)?;
        if let Some(record) = store.get(id) {
            updated.push(record.clone());
        }
    }
    Ok(updated)
}
