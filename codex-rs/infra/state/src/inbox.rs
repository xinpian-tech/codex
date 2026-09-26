use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use serde::Deserialize;
use serde::Serialize;

use crate::Journal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresentedInput {
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone)]
pub struct InboxEntry {
    pub message: AgentMessage,
    pub accepted_sequence: u64,
    pub presented: Option<PresentedInput>,
    pub presented_sequence: Option<u64>,
    pub outcome_ref: Option<String>,
    pub processed_sequence: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum InboxEvent {
    Accepted {
        message: Box<AgentMessage>,
    },
    Presented {
        message_id: MessageId,
        input: PresentedInput,
    },
    Processed {
        message_id: MessageId,
        outcome_ref: String,
    },
}

/// The receiving host calls `accept` only with messages assembled from its own
/// tmux stdin. Delivery and model presentation have distinct durable records.
pub struct DurableInbox {
    journal: Journal,
    root_session_id: RootSessionId,
    agent_id: AgentId,
    entries: BTreeMap<MessageId, InboxEntry>,
    pending: BTreeMap<u64, MessageId>,
}

impl DurableInbox {
    pub fn open(
        path: &Path,
        root_session_id: RootSessionId,
        agent_id: AgentId,
    ) -> io::Result<Self> {
        let mut entries = BTreeMap::new();
        let mut pending = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            let event: InboxEvent =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            if let InboxEvent::Accepted { message } = &event
                && (message.root_session_id != root_session_id || message.to.agent_id != agent_id)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "inbox belongs to another Agent",
                ));
            }
            apply(&mut entries, &mut pending, event, record.sequence)
        })?;
        Ok(Self {
            journal,
            root_session_id,
            agent_id,
            entries,
            pending,
        })
    }

    pub fn get(&self, message_id: MessageId) -> Option<&InboxEntry> {
        self.entries.get(&message_id)
    }

    /// The scheduler reads accepted order and selects relevant inputs in bounded
    /// batches. Body retrieval stays inside this receiving Agent's host.
    pub fn pending_from(
        &self,
        first_sequence: u64,
        limit: NonZeroUsize,
    ) -> impl Iterator<Item = &InboxEntry> {
        self.pending
            .range(first_sequence..)
            .take(limit.get())
            .filter_map(|(_, message_id)| self.entries.get(message_id))
    }

    pub fn accept(&mut self, message: AgentMessage) -> io::Result<u64> {
        if message.root_session_id != self.root_session_id || message.to.agent_id != self.agent_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message addressed to another Agent",
            ));
        }
        if let Some(entry) = self.entries.get(&message.message_id) {
            return if entry.message == message {
                Ok(entry.accepted_sequence)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "message ID refers to different content",
                ))
            };
        }
        self.append(InboxEvent::Accepted {
            message: Box::new(message),
        })
    }

    /// Call after confirming this message ID is in durable thread history. On
    /// recovery, reconcile history IDs before submitting pending inputs again.
    pub fn mark_presented(
        &mut self,
        message_id: MessageId,
        input: PresentedInput,
    ) -> io::Result<u64> {
        let entry = self
            .entries
            .get(&message_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "message not accepted"))?;
        if entry
            .presented
            .as_ref()
            .is_some_and(|previous| previous != &input)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message already presented in another turn",
            ));
        }
        if let Some(sequence) = entry.presented_sequence {
            return Ok(sequence);
        }
        self.append(InboxEvent::Presented { message_id, input })
    }

    pub fn mark_processed(
        &mut self,
        message_id: MessageId,
        outcome_ref: String,
    ) -> io::Result<u64> {
        let entry = self
            .entries
            .get(&message_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "message not accepted"))?;
        if entry.presented.is_none()
            || entry
                .outcome_ref
                .as_ref()
                .is_some_and(|previous| previous != &outcome_ref)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message outcome does not match presentation state",
            ));
        }
        if let Some(sequence) = entry.processed_sequence {
            return Ok(sequence);
        }
        self.append(InboxEvent::Processed {
            message_id,
            outcome_ref,
        })
    }

    fn append(&mut self, event: InboxEvent) -> io::Result<u64> {
        let sequence = self
            .journal
            .append(&serde_json::to_vec(&event).map_err(io::Error::other)?)?;
        apply(&mut self.entries, &mut self.pending, event, sequence)?;
        Ok(sequence)
    }
}

fn apply(
    entries: &mut BTreeMap<MessageId, InboxEntry>,
    pending: &mut BTreeMap<u64, MessageId>,
    event: InboxEvent,
    sequence: u64,
) -> io::Result<()> {
    match event {
        InboxEvent::Accepted { message } => {
            if entries.contains_key(&message.message_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate inbox acceptance",
                ));
            }
            pending.insert(sequence, message.message_id);
            entries.insert(
                message.message_id,
                InboxEntry {
                    message: *message,
                    accepted_sequence: sequence,
                    presented: None,
                    presented_sequence: None,
                    outcome_ref: None,
                    processed_sequence: None,
                },
            );
        }
        InboxEvent::Presented { message_id, input } => {
            let entry = entries.get_mut(&message_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "presentation without acceptance",
                )
            })?;
            entry.presented = Some(input);
            entry.presented_sequence = Some(sequence);
            pending.remove(&entry.accepted_sequence);
        }
        InboxEvent::Processed {
            message_id,
            outcome_ref,
        } => {
            let entry = entries.get_mut(&message_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "outcome without acceptance")
            })?;
            entry.outcome_ref = Some(outcome_ref);
            entry.processed_sequence = Some(sequence);
        }
    }
    Ok(())
}
