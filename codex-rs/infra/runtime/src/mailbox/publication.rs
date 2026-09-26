use std::io;
use std::io::Write;
use std::num::NonZeroUsize;

use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MessageId;
use codex_infra_state::QueueItem;

use super::HostMailbox;

const PUBLICATION_LANE: &str = "publication";

impl<W: Write> HostMailbox<W> {
    /// Persists the semantic envelope before waiting for its worktree. Commit
    /// is a draft here; only the later checkpoint determines the sent commit.
    pub fn stage_publication(&mut self, message: &AgentMessage) -> io::Result<()> {
        if message.root_session_id != self.root_session_id || message.from.agent_id != self.agent_id
        {
            return Err(io::Error::other("publication belongs to another Agent"));
        }
        self.publications.enqueue(QueueItem {
            key: message.message_id.to_string(),
            lane: PUBLICATION_LANE.to_owned(),
            payload: serde_json::to_vec(message).map_err(io::Error::other)?,
        })?;
        Ok(())
    }

    /// Reads a bounded batch of unresolved intents. Completed payloads remain
    /// on disk; recovery repeatedly consumes batches after workspace recovery.
    pub fn pending_publications(&self, limit: NonZeroUsize) -> io::Result<Vec<AgentMessage>> {
        self.publications
            .pending_keys(PUBLICATION_LANE, /*first_sequence*/ 0, limit)
            .into_iter()
            .map(|key| {
                let item = self.publications.read(key)?;
                let message: AgentMessage =
                    serde_json::from_slice(&item.payload).map_err(io::Error::other)?;
                if message.message_id.to_string() != key
                    || message.root_session_id != self.root_session_id
                    || message.from.agent_id != self.agent_id
                {
                    return Err(io::Error::other("publication journal binding changed"));
                }
                Ok(message)
            })
            .collect()
    }

    /// Reconciles the crash window between outbox enqueue and intent completion.
    /// Only commit may differ from the draft; the existing envelope is retained.
    /// None means checkpoint/enqueue has not happened yet. Terminal delivery and
    /// receiver acknowledgement remain owned by the outbox's replay protocol.
    pub fn reconcile_publication(&mut self, message_id: MessageId) -> io::Result<Option<u64>> {
        let Some(entry) = self.outbox.get(message_id) else {
            return Ok(None);
        };
        let key = message_id.to_string();
        let item = self.publications.read(&key)?;
        let mut draft: AgentMessage =
            serde_json::from_slice(&item.payload).map_err(io::Error::other)?;
        draft.commit = entry.message.commit.clone();
        if draft != entry.message {
            return Err(io::Error::other(
                "outbox envelope differs from publication intent",
            ));
        }
        let sequence = entry.queued_sequence;
        self.publications.complete(&key)?;
        Ok(Some(sequence))
    }
}
