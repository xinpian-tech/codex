use std::net::SocketAddr;

use serde::Deserialize;
use serde::Serialize;

use crate::AgentId;
use crate::CommitId;
use crate::MachineId;
use crate::MessageKind;
use crate::RootSessionId;
use crate::TaskId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleDefinition {
    pub role_id: String,
    pub responsibility: String,
    pub accepts_from_roles: Vec<String>,
    pub routes_to_roles: Vec<String>,
    pub accepted_input_kinds: Vec<MessageKind>,
    pub produced_output_kinds: Vec<MessageKind>,
    pub subscribed_events: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Starting,
    Running,
    Waiting,
    Finalizing,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDescriptor {
    pub agent_id: AgentId,
    pub parent_agent_id: Option<AgentId>,
    pub root_session_id: RootSessionId,
    pub revision: u64,
    pub updated_at: i64,
    pub role: String,
    pub responsibility: String,
    pub task_id: TaskId,
    pub status: AgentStatus,
    pub machine_id: MachineId,
    pub repo: String,
    pub commit: CommitId,
    pub tmux_session: String,
    pub tmux_window: String,
    pub tmux_pane: String,
    pub tmux_endpoint: SocketAddr,
    pub receives_from: Vec<AgentId>,
    pub sends_to: Vec<AgentId>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum DirectoryPublisher {
    Machine(MachineId),
    Agent(AgentId),
}

/// The owning machine serializes directory updates into one persistent stream
/// per Root Session, preserving the Agent or runtime that originated the change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEvent {
    pub source_machine_id: MachineId,
    pub publisher: DirectoryPublisher,
    pub source_sequence: u64,
    pub descriptor: AgentDescriptor,
}
