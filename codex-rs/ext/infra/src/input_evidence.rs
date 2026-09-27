use std::collections::BTreeMap;
use std::io;

use codex_infra_state::JournalPosition;
use serde_json::Value;

use crate::AgentMessageInput;
use crate::StoreAuditEvent;
use crate::StoreAuditPage;

/// Historical store evidence, not proof of model consumption or task completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentInputEvidenceStatus {
    AwaitingAppend,
    AwaitingFlush,
    Flushed { finished_sequence: u64 },
}

#[derive(Clone)]
enum Pending {
    Append(u16),
    Flush,
}

/// Incrementally reconciles one envelope against raw store audit pages.
/// Rebuild from sequence zero after restart until the owning submission journal
/// has its own durable checkpoint. Compaction does not erase this evidence.
/// A successful flush must start after all matching appends have succeeded.
#[derive(Clone)]
pub struct AgentInputEvidence {
    thread_id: String,
    expected: Vec<Value>,
    appended: u16,
    pending: BTreeMap<u64, Pending>,
    cursor: JournalPosition,
    flushed: Option<u64>,
}

impl AgentInputEvidence {
    pub(crate) fn since(
        input: &AgentMessageInput,
        thread_id: String,
        cursor: JournalPosition,
    ) -> io::Result<Self> {
        let mut evidence = Self::new(input, thread_id)?;
        evidence.cursor = cursor;
        Ok(evidence)
    }

    pub fn new(input: &AgentMessageInput, thread_id: String) -> io::Result<Self> {
        Ok(Self {
            thread_id,
            expected: input.response_items()?,
            appended: 0,
            pending: BTreeMap::new(),
            cursor: JournalPosition::default(),
            flushed: None,
        })
    }

    pub fn cursor(&self) -> JournalPosition {
        self.cursor
    }

    pub fn status(&self) -> AgentInputEvidenceStatus {
        if let Some(finished_sequence) = self.flushed {
            AgentInputEvidenceStatus::Flushed { finished_sequence }
        } else if self.appended == (1_u16 << self.expected.len()) - 1 {
            AgentInputEvidenceStatus::AwaitingFlush
        } else {
            AgentInputEvidenceStatus::AwaitingAppend
        }
    }

    /// Applies a complete contiguous page atomically; an error leaves the
    /// previous cursor and evidence intact. Unfinished or failed operations
    /// never count as a successful append or flush.
    pub fn observe(&mut self, page: &StoreAuditPage) -> io::Result<AgentInputEvidenceStatus> {
        let mut next = self.clone();
        let mut sequence = self.cursor.next_sequence;
        for record in &page.records {
            if record.sequence != sequence {
                return Err(io::Error::other(
                    "input evidence audit page is not contiguous",
                ));
            }
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("input evidence sequence exhausted"))?;
            match &record.event {
                StoreAuditEvent::Opened { .. } => {}
                StoreAuditEvent::Started { operation, payload } => {
                    let pending = match operation.as_str() {
                        "append_items" => {
                            let (thread, items): (String, Vec<Value>) =
                                serde_json::from_value(payload.clone())?;
                            let mut mask = 0;
                            if thread == next.thread_id {
                                for item in items {
                                    if item.get("type").and_then(Value::as_str)
                                        != Some("response_item")
                                    {
                                        continue;
                                    }
                                    let Some(actual) = item.get("payload") else {
                                        return Err(io::Error::other(
                                            "response item has no payload",
                                        ));
                                    };
                                    for (index, expected) in next.expected.iter().enumerate() {
                                        if actual.get("id") == expected.get("id") {
                                            if actual != expected {
                                                return Err(io::Error::other(
                                                    "Agent input ID has different stored content",
                                                ));
                                            }
                                            mask |= 1_u16 << index;
                                        }
                                    }
                                }
                            }
                            (mask != 0).then_some(Pending::Append(mask))
                        }
                        "flush_thread" => (payload.as_str() == Some(&next.thread_id)
                            && next.status() == AgentInputEvidenceStatus::AwaitingFlush)
                            .then_some(Pending::Flush),
                        // The existing local store implements preparation as a
                        // flush. Other persist contexts can be no-ops and are
                        // intentionally not treated as flush evidence here.
                        "persist_thread" => {
                            let (thread, context): (String, String) =
                                serde_json::from_value(payload.clone())?;
                            (thread == next.thread_id
                                && context == "thread_preparation"
                                && next.status() == AgentInputEvidenceStatus::AwaitingFlush)
                                .then_some(Pending::Flush)
                        }
                        _ => None,
                    };
                    if let Some(pending) = pending {
                        // Bound retained state even when many repeated writes
                        // never report their outcome. The owning driver must
                        // reconcile those operations before continuing.
                        if next.pending.len() == 128 {
                            return Err(io::Error::other(
                                "input evidence has too many pending writes",
                            ));
                        }
                        next.pending.insert(record.sequence, pending);
                    }
                }
                StoreAuditEvent::Finished {
                    started_sequence,
                    outcome,
                } => {
                    if let Some(pending) = next.pending.remove(started_sequence)
                        && outcome.is_ok()
                    {
                        match pending {
                            Pending::Append(mask) => next.appended |= mask,
                            Pending::Flush => {
                                next.flushed.get_or_insert(record.sequence);
                            }
                        }
                    }
                }
            }
        }
        if sequence != page.next.next_sequence
            || page.next.byte_offset < self.cursor.byte_offset
            || page.next.next_sequence > page.durable_end.next_sequence
            || page.next.byte_offset > page.durable_end.byte_offset
        {
            return Err(io::Error::other(
                "input evidence page boundary is inconsistent",
            ));
        }
        next.cursor = page.next;
        let status = next.status();
        *self = next;
        Ok(status)
    }
}
