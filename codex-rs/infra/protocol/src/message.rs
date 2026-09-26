use serde::Deserialize;
use serde::Serialize;

use crate::AgentId;
use crate::AssignmentId;
use crate::CommitId;
use crate::MachineId;
use crate::MessageId;
use crate::RootSessionId;
use crate::TaskId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAddress {
    pub agent_id: AgentId,
    pub role: String,
    pub machine_id: MachineId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Bootstrap,
    Task,
    Progress,
    WorkingContext,
    Contribution,
    Escalation,
    Result,
}

/// The receiver applies role/task relevance before choosing an input boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Presentation {
    NextInputBoundary,
    NextTurn,
    Archive,
}

/// Semantic messages are encoded by the host and sent through both tmux panes.
/// The host supplies identities and its actual pushed HEAD, including for replies
/// quoting entries that were originally authored by another Agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMessage {
    pub message_id: MessageId,
    pub root_session_id: RootSessionId,
    pub from: MessageAddress,
    pub to: MessageAddress,
    pub repo: String,
    pub commit: CommitId,
    pub task_id: TaskId,
    pub assignment_id: AssignmentId,
    pub kind: MessageKind,
    pub presentation: Presentation,
    pub reply_to: Option<MessageId>,
    pub body: String,
}
