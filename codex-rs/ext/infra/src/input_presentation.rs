use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;

use codex_infra_protocol::MessageId;
use codex_infra_state::JournalPosition;
use codex_protocol::models::ResponseItem;
use serde::Deserialize;
use serde::Serialize;

use crate::AgentMessageInput;
use crate::ModelInputAuditEvent;
use crate::ModelInputAuditPage;

/// A completed generation whose input, including recorded continuation ancestry,
/// contains every fragment. This is presentation evidence, not Task completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInputPresentation {
    pub message_id: MessageId,
    pub thread_id: String,
    pub turn_id: String,
    pub attempt_id: MessageId,
    pub response_id: String,
    pub prepared_sequence: u64,
    pub completed_sequence: u64,
}

#[derive(Clone)]
struct Attempt {
    mask: u16,
    warmup: bool,
    turn_id: Option<String>,
    prepared_sequence: u64,
}

/// Per-envelope incremental reconciliation against the model-input audit.
/// Missing responses and warmup alone cannot establish presentation. Persist
/// the returned evidence separately; rebuilding starts at journal sequence zero.
#[derive(Clone)]
pub struct AgentInputPresentationScan {
    message_id: MessageId,
    thread_id: String,
    expected: Vec<ResponseItem>,
    attempts: BTreeMap<MessageId, Attempt>,
    responses: BTreeMap<String, u16>,
    limit: NonZeroUsize,
    cursor: JournalPosition,
    presentation: Option<AgentInputPresentation>,
}

impl AgentInputPresentationScan {
    pub fn new(
        input: &AgentMessageInput,
        thread_id: String,
        limit: NonZeroUsize,
    ) -> io::Result<Self> {
        let expected = input
            .response_items()?
            .into_iter()
            .map(|value| {
                let mut item: ResponseItem = serde_json::from_value(value)?;
                // Providers may omit internal content kinds. Preserve all other
                // fields, including the stable item ID and exact fragment text.
                item.clear_content_item_kinds();
                Ok(item)
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            message_id: input.message_id(),
            thread_id,
            expected,
            attempts: BTreeMap::new(),
            responses: BTreeMap::new(),
            limit,
            cursor: JournalPosition::default(),
            presentation: None,
        })
    }

    pub fn cursor(&self) -> JournalPosition {
        self.cursor
    }

    pub fn presentation(&self) -> Option<&AgentInputPresentation> {
        self.presentation.as_ref()
    }

    /// A page is applied atomically. Limits bound retained relevant attempts
    /// and continuation links; unrelated requests do not occupy these maps.
    pub fn observe(&mut self, page: &ModelInputAuditPage) -> io::Result<()> {
        if page.start != self.cursor {
            return Err(io::Error::other(
                "presentation audit page is not contiguous",
            ));
        }
        let mut next = self.clone();
        let mut sequence = page.start.next_sequence;
        for record in &page.records {
            if record.sequence != sequence {
                return Err(io::Error::other("presentation audit sequence changed"));
            }
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("presentation sequence exhausted"))?;
            if next.presentation.is_some() {
                continue;
            }
            match &record.event {
                ModelInputAuditEvent::Prepared {
                    attempt_id,
                    thread_id,
                    turn_id,
                    warmup,
                    previous_response_id,
                    input,
                    ..
                } => {
                    if thread_id != &next.thread_id {
                        continue;
                    }
                    let mut mask = previous_response_id
                        .as_ref()
                        .and_then(|id| next.responses.get(id))
                        .copied()
                        .unwrap_or_default();
                    for actual in input {
                        for (index, expected) in next.expected.iter().enumerate() {
                            if actual.id() == expected.id() {
                                let mut normalized = actual.clone();
                                normalized.clear_content_item_kinds();
                                if &normalized != expected {
                                    return Err(io::Error::other(
                                        "model input fragment content changed",
                                    ));
                                }
                                mask |= 1_u16 << index;
                            }
                        }
                    }
                    if mask != 0 {
                        if next.attempts.len() >= next.limit.get()
                            || next.attempts.contains_key(attempt_id)
                        {
                            return Err(io::Error::other(
                                "presentation attempt capacity or identity changed",
                            ));
                        }
                        next.attempts.insert(
                            *attempt_id,
                            Attempt {
                                mask,
                                warmup: *warmup,
                                turn_id: turn_id.clone(),
                                prepared_sequence: record.sequence,
                            },
                        );
                    }
                }
                ModelInputAuditEvent::Completed {
                    attempt_id,
                    response_id,
                    ..
                } => {
                    let Some(attempt) = next.attempts.remove(attempt_id) else {
                        continue;
                    };
                    if response_id.is_empty() {
                        return Err(io::Error::other(
                            "completed presentation response has no ID",
                        ));
                    }
                    if let Some(previous) = next.responses.get(response_id) {
                        if *previous != attempt.mask {
                            return Err(io::Error::other("presentation response ancestry changed"));
                        }
                    } else {
                        if next.responses.len() >= next.limit.get() {
                            return Err(io::Error::other(
                                "presentation continuation capacity exceeded",
                            ));
                        }
                        next.responses.insert(response_id.clone(), attempt.mask);
                    }
                    if !attempt.warmup
                        && attempt.mask == (1_u16 << next.expected.len()) - 1
                        && let Some(turn_id) = attempt.turn_id.filter(|id| !id.is_empty())
                    {
                        next.presentation = Some(AgentInputPresentation {
                            message_id: next.message_id,
                            thread_id: next.thread_id.clone(),
                            turn_id,
                            attempt_id: *attempt_id,
                            response_id: response_id.clone(),
                            prepared_sequence: attempt.prepared_sequence,
                            completed_sequence: record.sequence,
                        });
                    }
                }
                ModelInputAuditEvent::StreamFailed { attempt_id, .. }
                | ModelInputAuditEvent::StreamEnded { attempt_id } => {
                    next.attempts.remove(attempt_id);
                }
                ModelInputAuditEvent::Opened { .. }
                | ModelInputAuditEvent::Created { .. }
                | ModelInputAuditEvent::ServerModel { .. } => {}
            }
        }
        if sequence != page.next.next_sequence
            || page.next.byte_offset < page.start.byte_offset
            || page.next.next_sequence > page.durable_end.next_sequence
            || page.next.byte_offset > page.durable_end.byte_offset
        {
            return Err(io::Error::other(
                "presentation audit boundary is inconsistent",
            ));
        }
        next.cursor = page.next;
        *self = next;
        Ok(())
    }
}
