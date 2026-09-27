use std::io;

use codex_protocol::models::ContentItemKind;

use super::ContextualUserFragment;

/// One part of a tmux-delivered Agent envelope. Keep parts as separate context
/// items in order; their payloads concatenate to the original envelope JSON.
#[derive(Clone, Debug)]
pub struct AgentInputFragment {
    body: String,
}

impl AgentInputFragment {
    pub const MAX_PARTS: usize = 8;
    pub const PAYLOAD_BYTES: usize = 640;

    /// Each rendered part, including its markers, is at most 900 bytes. The
    /// complete message has at most eight parts and retains its original ID.
    pub fn new(message_id: &str, part: usize, total: usize, payload: &str) -> io::Result<Self> {
        if message_id.is_empty()
            || message_id.len() > 64
            || total == 0
            || total > Self::MAX_PARTS
            || part == 0
            || part > total
            || payload.len() > Self::PAYLOAD_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid Agent input part",
            ));
        }
        let fragment = Self {
            body: format!(
                "Received Agent envelope {message_id}, part {part}/{total}. Concatenate payload parts in order to read the envelope JSON.\n{payload}"
            ),
        };
        if fragment.render().len() > 900 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Agent input part exceeds 900 bytes",
            ));
        }
        Ok(fragment)
    }
}

impl ContextualUserFragment for AgentInputFragment {
    fn role(&self) -> &'static str {
        "user"
    }

    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("infra.agent_input".to_owned())
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("<agent_input>\n", "\n</agent_input>")
    }

    fn body(&self) -> String {
        self.body.clone()
    }
}
