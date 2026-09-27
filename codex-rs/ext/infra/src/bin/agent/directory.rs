use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use codex_infra_protocol::AgentDescriptor;
use codex_infra_protocol::AgentId;
use codex_infra_protocol::DirectoryEvent;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::RoleDefinition;
use serde_json::Value;
use serde_json::json;

use super::run::RunConfig;

pub fn read(path: &Path) -> io::Result<BTreeMap<AgentId, AgentDescriptor>> {
    let events: Vec<DirectoryEvent> = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut agents = BTreeMap::new();
    for event in events {
        agents.insert(event.descriptor.agent_id, event.descriptor);
    }
    Ok(agents)
}

/// Called by the live directory tool. Pages keep unrelated status changes out
/// of the model's input; the model requests the next page when it needs it.
pub fn query(config: &RunConfig, role: &RoleDefinition, arguments: &Value) -> io::Result<String> {
    let view = arguments["view"].as_str().unwrap_or("agents");
    let after = arguments["after"].as_str().unwrap_or("");
    let candidates: Vec<(String, Value)> = match view {
        "agents" => read(&config.directory_file)?.into_values()
            .filter(|agent| arguments["role"].as_str().map_or_else(
                || role.routes_to_roles.contains(&agent.role) || role.accepts_from_roles.contains(&agent.role) || role.role_id == agent.role,
                |selected| selected == agent.role,
            ))
            .filter(|agent| arguments["task_id"].as_str().is_none_or(|task| task == agent.task_id.to_string()))
            .map(|agent| (agent.agent_id.to_string(), json!({
                "agent_id":agent.agent_id,"role":agent.role,"machine_id":agent.machine_id,
                "task_id":agent.task_id,"status":agent.status,"repo":agent.repo,"commit":agent.commit,
                "responsibility":agent.responsibility
            }))).collect(),
        "profiles" => config.worker_profiles.iter().map(|(name, profile)| (name.clone(), json!({"name":name,"inference":profile.generation.inference,"machines":profile.repositories.keys().collect::<Vec<_>>(),"task_repository":profile.task_repository}))).collect(),
        "machines" => {
            let machines: BTreeMap<MachineId, std::net::SocketAddr> = serde_json::from_slice(&std::fs::read(&config.machines_file)?)?;
            machines.into_keys().map(|machine| (machine.to_string(), json!({"machine_id":machine}))).collect()
        }
        _ => return Err(io::Error::other("directory view must be agents, profiles or machines")),
    };
    let mut candidates: Vec<_> = candidates
        .into_iter()
        .filter(|(key, _)| key.as_str() > after)
        .collect();
    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    let mut entries = Vec::new();
    let mut last = None;
    let mut more = false;
    for (key, value) in candidates {
        entries.push(value);
        if entries.len() > 4 || serde_json::to_vec(&entries)?.len() > 2400 {
            entries.pop();
            if entries.is_empty() {
                return Err(io::Error::other(
                    "directory entry exceeds a model page; inspect the catalog file with the file tools",
                ));
            }
            more = true;
            break;
        }
        last = Some(key);
    }
    Ok(serde_json::to_string(
        &json!({"view":view,"entries":entries,"next":if more {last} else {None}}),
    )?)
}
