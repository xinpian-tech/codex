use std::io;

use codex_core::context::AgentInputFragment;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::Presentation;
use codex_infra_runtime::LaunchIntent;
use codex_infra_state::InboxEntry;

/// Bounded model input prepared from an already accepted terminal inbox entry.
/// Construction neither submits a turn nor acknowledges message presentation.
pub struct AgentMessageInput {
    message_id: MessageId,
    fragments: Vec<AgentInputFragment>,
}

impl AgentMessageInput {
    pub fn from_inbox(entry: &InboxEntry, launch: &LaunchIntent) -> io::Result<Self> {
        let message = &entry.message;
        if message.root_session_id != launch.workspace.root_session_id
            || message.to.agent_id != launch.workspace.agent_id
            || message.to.machine_id != launch.machine_id
            || message.to.role != launch.role
        {
            return Err(io::Error::other(
                "Agent input recipient differs from launch",
            ));
        }
        if message.presentation == Presentation::Archive {
            return Err(io::Error::other(
                "archive-only message has no model presentation",
            ));
        }
        let encoded = serde_json::to_string(message)?;
        let mut remaining = encoded.as_str();
        let mut parts = Vec::new();
        while !remaining.is_empty() {
            if parts.len() == AgentInputFragment::MAX_PARTS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Agent envelope exceeds input budget; send a bounded summary with references",
                ));
            }
            let end = remaining
                .floor_char_boundary(remaining.len().min(AgentInputFragment::PAYLOAD_BYTES));
            let (part, rest) = remaining.split_at(end);
            parts.push(part);
            remaining = rest;
        }
        let message_id = message.message_id;
        let id = message_id.to_string();
        let total = parts.len();
        let fragments = parts
            .into_iter()
            .enumerate()
            .map(|(index, payload)| AgentInputFragment::new(&id, index + 1, total, payload))
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            message_id,
            fragments,
        })
    }

    /// Use as client_user_message_id, then reconcile against durable turn items
    /// before sending the mailbox's Presented receipt or retrying submission.
    pub fn message_id(&self) -> MessageId {
        self.message_id
    }

    pub fn fragments(&self) -> &[AgentInputFragment] {
        &self.fragments
    }
}
