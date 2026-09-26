use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::DeliveryReceipt;
use codex_infra_protocol::DeliveryStage;
use codex_infra_protocol::FrameRoute;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use serde::Deserialize;
use serde::Serialize;

use crate::Journal;

#[derive(Debug, Clone)]
pub struct OutboxEntry {
    pub message: AgentMessage,
    pub queued_sequence: u64,
    pub accepted_sequence: Option<u64>,
    pub presented_sequence: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum OutboxEvent {
    Queued { message: Box<AgentMessage> },
    Receipt(DeliveryReceipt),
}

/// The sending host persists an envelope here before writing it to stdout.
/// Pending entries are replayed through that same stdout/tmux path after restart.
pub struct DurableOutbox {
    journal: Journal,
    root_session_id: RootSessionId,
    agent_id: AgentId,
    entries: BTreeMap<MessageId, OutboxEntry>,
    pending: BTreeMap<u64, MessageId>,
}

impl DurableOutbox {
    pub fn open(
        path: &Path,
        root_session_id: RootSessionId,
        agent_id: AgentId,
    ) -> io::Result<Self> {
        let mut entries = BTreeMap::new();
        let mut pending = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            let event: OutboxEvent =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            if let OutboxEvent::Queued { message } = &event
                && (message.root_session_id != root_session_id || message.from.agent_id != agent_id)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "outbox belongs to another Agent",
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

    pub fn get(&self, message_id: MessageId) -> Option<&OutboxEntry> {
        self.entries.get(&message_id)
    }

    pub fn pending_from(
        &self,
        first_sequence: u64,
        limit: NonZeroUsize,
    ) -> impl Iterator<Item = &OutboxEntry> {
        self.pending
            .range(first_sequence..)
            .take(limit.get())
            .filter_map(|(_, message_id)| self.entries.get(message_id))
    }

    /// Reusing an ID is an idempotent retry only when the whole envelope matches.
    /// The host stamps the pushed checkpoint commit before calling this method.
    pub fn enqueue(&mut self, message: AgentMessage) -> io::Result<u64> {
        if message.root_session_id != self.root_session_id || message.from.agent_id != self.agent_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message belongs to another sender",
            ));
        }
        if let Some(entry) = self.entries.get(&message.message_id) {
            return if entry.message == message {
                Ok(entry.queued_sequence)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "message ID refers to different content",
                ))
            };
        }
        self.append(OutboxEvent::Queued {
            message: Box::new(message),
        })
    }

    /// Returns whether a new receipt was recorded. A presented receipt also ends
    /// retransmission if it arrives before the accepted receipt on reconnect.
    pub fn acknowledge(&mut self, receipt: DeliveryReceipt) -> io::Result<bool> {
        let entry = self.entries.get(&receipt.route.message_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "receipt without queued message")
        })?;
        if !validate_receipt(entry, &receipt)? {
            return Ok(false);
        }
        self.append(OutboxEvent::Receipt(receipt))?;
        Ok(true)
    }

    fn append(&mut self, event: OutboxEvent) -> io::Result<u64> {
        let sequence = self
            .journal
            .append(&serde_json::to_vec(&event).map_err(io::Error::other)?)?;
        apply(&mut self.entries, &mut self.pending, event, sequence)?;
        Ok(sequence)
    }
}

fn validate_receipt(entry: &OutboxEntry, receipt: &DeliveryReceipt) -> io::Result<bool> {
    if FrameRoute::from(&entry.message) != receipt.route {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "receipt route differs from queued message",
        ));
    }
    let (previous, ordered) = match receipt.stage {
        DeliveryStage::Accepted => (
            entry.accepted_sequence,
            entry
                .presented_sequence
                .is_none_or(|sequence| receipt.durable_sequence < sequence),
        ),
        DeliveryStage::Presented => (
            entry.presented_sequence,
            entry
                .accepted_sequence
                .is_none_or(|sequence| sequence < receipt.durable_sequence),
        ),
    };
    if !ordered || previous.is_some_and(|sequence| sequence != receipt.durable_sequence) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "receipt differs from recorded inbox history",
        ));
    }
    Ok(previous.is_none())
}

fn apply(
    entries: &mut BTreeMap<MessageId, OutboxEntry>,
    pending: &mut BTreeMap<u64, MessageId>,
    event: OutboxEvent,
    sequence: u64,
) -> io::Result<()> {
    match event {
        OutboxEvent::Queued { message } => {
            if entries.contains_key(&message.message_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate outbox message",
                ));
            }
            pending.insert(sequence, message.message_id);
            entries.insert(
                message.message_id,
                OutboxEntry {
                    message: *message,
                    queued_sequence: sequence,
                    accepted_sequence: None,
                    presented_sequence: None,
                },
            );
        }
        OutboxEvent::Receipt(receipt) => {
            let entry = entries.get_mut(&receipt.route.message_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "receipt without queued message")
            })?;
            validate_receipt(entry, &receipt)?;
            match receipt.stage {
                DeliveryStage::Accepted => entry.accepted_sequence = Some(receipt.durable_sequence),
                DeliveryStage::Presented => {
                    entry.presented_sequence = Some(receipt.durable_sequence)
                }
            }
            pending.remove(&entry.queued_sequence);
        }
    }
    Ok(())
}
