use super::run::RunConfig;

pub async fn start_ready(
    host: &StartedAgentHost,
    config: &RunConfig,
    bootstrap: &codex_infra_state::InboxEntry,
    state: &mut super::run::LoopState,
) -> io::Result<()> {
    for (task_id, mut arguments) in state.waiting_tasks.clone() {
        let records = request(
            config,
            &host.launch.machine_id,
            TaskCommand::Get { task_id },
        )
        .await?;
        if records
            .first()
            .is_some_and(|task| task.status() == TaskStatus::Ready)
        {
            arguments["task_id"] = serde_json::to_value(task_id)?;
            super::spawn::spawn(host, config, bootstrap, &arguments, state).await?;
        }
    }
    Ok(())
}
use codex_infra_extension::StartedAgentHost;
use codex_infra_protocol::*;
use codex_infra_runtime::{LaunchServiceRequest, TaskCommand, launch_request};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;

pub async fn request(
    config: &RunConfig,
    owner: &MachineId,
    command: TaskCommand,
) -> io::Result<Vec<TaskRecord>> {
    let machines: BTreeMap<MachineId, SocketAddr> =
        serde_json::from_slice(&std::fs::read(&config.machines_file)?)?;
    let endpoint = *machines
        .get(owner)
        .ok_or_else(|| io::Error::other("Task owner endpoint missing"))?;
    Ok(launch_request(
        endpoint,
        LaunchServiceRequest::Task {
            command: Box::new(command),
        },
    )
    .await?
    .tasks)
}

pub async fn query(
    host: &StartedAgentHost,
    config: &RunConfig,
    arguments: &Value,
) -> io::Result<String> {
    let command = if arguments["task_id"].is_null() {
        TaskCommand::List {
            after: serde_json::from_value(arguments["after"].clone())?,
        }
    } else {
        TaskCommand::Get {
            task_id: serde_json::from_value(arguments["task_id"].clone())?,
        }
    };
    let records = request(config, &host.launch.machine_id, command).await?;
    let more = records.len() > 4;
    let page: Vec<_> = records.into_iter().take(4).collect();
    Ok(serde_json::to_string(
        &json!({"tasks":page,"next":if more {page.last().map(TaskRecord::task_id)} else {None}}),
    )?)
}

pub async fn observe(
    host: &StartedAgentHost,
    config: &RunConfig,
    message: &AgentMessage,
) -> io::Result<()> {
    if !matches!(message.kind, MessageKind::Result | MessageKind::Escalation) {
        return Ok(());
    }
    let records = request(
        config,
        &host.launch.machine_id,
        TaskCommand::Get {
            task_id: message.task_id,
        },
    )
    .await?;
    let Some(record) = records.first() else {
        return Ok(());
    };
    if !record.assignments().iter().any(|assignment| {
        assignment.assignment_id == message.assignment_id
            && assignment.agent_id == message.from.agent_id
    }) {
        return Ok(());
    }
    let command = if message.kind == MessageKind::Result {
        TaskCommand::Complete {
            task_id: message.task_id,
            assignment_id: message.assignment_id,
            agent_id: message.from.agent_id,
            commit: message.commit.clone(),
        }
    } else {
        TaskCommand::Handoff {
            task_id: message.task_id,
            assignment_id: message.assignment_id,
            agent_id: message.from.agent_id,
            commit: message.commit.clone(),
        }
    };
    let updated = request(config, &host.launch.machine_id, command).await?;
    std::fs::write(
        host.directory
            .join(format!("task-result-{}.json", message.message_id)),
        serde_json::to_vec_pretty(&updated)?,
    )?;
    Ok(())
}
