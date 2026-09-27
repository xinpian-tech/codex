use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;

use codex_infra_extension::StartedAgentHost;
use codex_infra_protocol::*;
use codex_infra_runtime::LaunchServiceRequest;
use codex_infra_runtime::SpawnAgent;
use codex_infra_runtime::launch_request;
use codex_infra_state::InboxEntry;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::run::LoopState;
use super::run::RunConfig;

#[derive(Deserialize)]
pub struct WorkerProfile {
    pub generation: ConfigGeneration,
    pub host_program: PathBuf,
    pub repositories: BTreeMap<MachineId, PathBuf>,
    pub remote: String,
}

pub async fn spawn(
    host: &StartedAgentHost,
    config: &RunConfig,
    bootstrap: &InboxEntry,
    arguments: &Value,
    state: &mut LoopState,
) -> io::Result<String> {
    let machine: MachineId = serde_json::from_value(arguments["machine_id"].clone())?;
    let profile_name = arguments["profile"]
        .as_str()
        .ok_or_else(|| io::Error::other("profile required"))?;
    let profile = config
        .worker_profiles
        .get(profile_name)
        .ok_or_else(|| io::Error::other("unknown inference profile"))?;
    let machines: BTreeMap<MachineId, SocketAddr> =
        serde_json::from_slice(&std::fs::read(&config.machines_file)?)?;
    let local = *machines
        .get(&host.launch.machine_id)
        .ok_or_else(|| io::Error::other("local machine endpoint missing"))?;
    let remote = *machines
        .get(&machine)
        .ok_or_else(|| io::Error::other("target machine endpoint missing"))?;
    let mut task = host.task.clone();
    task.task_id = TaskId::new();
    task.assigned_agent = AgentId::new();
    task.owner_machine_id = host.launch.machine_id.clone();
    task.target_machine = machine.clone();
    task.revision = 0;
    task.objective = arguments["objective"]
        .as_str()
        .ok_or_else(|| io::Error::other("objective required"))?
        .to_owned();
    task.scope = arguments["scope"]
        .as_str()
        .ok_or_else(|| io::Error::other("scope required"))?
        .to_owned();
    task.nix_system = profile.generation.nix_system.clone();
    let role = arguments["role"]
        .as_str()
        .ok_or_else(|| io::Error::other("role required"))?
        .to_owned();
    let known = launch_request(local, LaunchServiceRequest::List).await?;
    let own = known
        .events
        .iter()
        .rev()
        .find(|event| event.descriptor.agent_id == host.launch.workspace.agent_id)
        .ok_or_else(|| io::Error::other("leader missing from machine directory"))?;
    task.source_commit = own.descriptor.commit.clone();
    let commit = std::process::Command::new(&host.generation.config.preparation.git)
        .current_dir(&host.launch.workspace.worktree)
        .args(["rev-parse", "HEAD"])
        .output()?;
    if !commit.status.success() {
        return Err(io::Error::other("cannot read current source commit"));
    }
    task.source_commit = String::from_utf8(commit.stdout)
        .map_err(io::Error::other)?
        .trim()
        .parse()
        .map_err(io::Error::other)?;
    if remote != local {
        launch_request(
            remote,
            LaunchServiceRequest::Sync {
                events: known.events,
            },
        )
        .await?;
    }
    let response = launch_request(
        remote,
        LaunchServiceRequest::Spawn {
            agent: Box::new(SpawnAgent {
                agent_id: task.assigned_agent,
                task_id: task.task_id,
                parent_agent_id: Some(host.launch.workspace.agent_id),
                role,
                repository: profile
                    .repositories
                    .get(&machine)
                    .ok_or_else(|| io::Error::other("repository missing on target machine"))?
                    .clone(),
                repo: task.repo.clone(),
                source_commit: task.source_commit.clone(),
                remote: profile.remote.clone(),
                generation: profile.generation.clone(),
                host_program: profile.host_program.clone(),
            }),
        },
    )
    .await?;
    let child = response
        .spawned
        .ok_or_else(|| io::Error::other("spawn returned no Agent"))?;
    if remote != local {
        launch_request(
            local,
            LaunchServiceRequest::Sync {
                events: response.events,
            },
        )
        .await?;
    }
    let connected = launch_request(
        local,
        LaunchServiceRequest::Connect {
            agent_id: host.launch.workspace.agent_id,
            peer: child.agent_id,
        },
    )
    .await?;
    if remote != local {
        launch_request(
            remote,
            LaunchServiceRequest::Sync {
                events: connected.events,
            },
        )
        .await?;
    }
    let mut message = super::events::message(
        host,
        bootstrap,
        MessageAddress {
            agent_id: child.agent_id,
            machine_id: child.machine_id.clone(),
            role: child.role.clone(),
        },
        MessageKind::Bootstrap,
        serde_json::to_string(&task)?,
    );
    message.task_id = task.task_id;
    message.assignment_id = AssignmentId::new();
    state.pending.push(message);
    Ok(serde_json::to_string(
        &json!({"agent":child,"task_id":task.task_id,"inference":profile.generation.inference,"delivery":"bootstrap queued for tmux at turn end"}),
    )?)
}
