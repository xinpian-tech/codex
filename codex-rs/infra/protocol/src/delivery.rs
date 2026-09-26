use serde::Deserialize;
use serde::Serialize;

use crate::AgentId;
use crate::AgentMessage;
use crate::MessageId;
use crate::RootSessionId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameRoute {
    pub message_id: MessageId,
    pub root_session_id: RootSessionId,
    pub from_agent_id: AgentId,
    pub to_agent_id: AgentId,
}

impl From<&AgentMessage> for FrameRoute {
    fn from(message: &AgentMessage) -> Self {
        Self {
            message_id: message.message_id,
            root_session_id: message.root_session_id,
            from_agent_id: message.from.agent_id,
            to_agent_id: message.to.agent_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStage {
    Accepted,
    Presented,
}

/// Metadata returned after the receiving host persists the corresponding event.
/// `route` describes the original message; receipts travel in reverse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryReceipt {
    pub route: FrameRoute,
    pub stage: DeliveryStage,
    pub durable_sequence: u64,
}
