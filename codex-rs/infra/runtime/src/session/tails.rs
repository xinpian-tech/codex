use std::io;
use std::num::NonZeroUsize;
use std::ops::Bound;

use codex_infra_protocol::MessageId;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;

use super::Attachment;
use super::CollectorOwner;
use super::TransportSession;
use crate::CollectorFinished;
use crate::ControlFrameReader;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CaptureTailState {
    AwaitingCompletion,
    Pending {
        staged: JournalPosition,
        target: JournalPosition,
    },
    Staged {
        position: JournalPosition,
        pending_forwarding: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureTailEntry {
    pub attachment_id: MessageId,
    pub state: CaptureTailState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureTailPage {
    pub entries: Vec<CaptureTailEntry>,
    pub next_cursor: Option<MessageId>,
}

impl TransportSession {
    /// After stop_capture, stages a bounded slice from each selected remaining
    /// attachment into its durable dispatch queue. Does not reconnect capture,
    /// send messages or inject input. Repeat Pending entries; paginate the stable
    /// attachment map with next_cursor. Previously retired attachments already
    /// reached their completion positions with empty dispatch queues.
    pub fn stage_capture_tails(
        &mut self,
        after: Option<MessageId>,
        limit: NonZeroUsize,
    ) -> io::Result<CaptureTailPage> {
        if !matches!(self.collector, CollectorOwner::Stopped(_)) {
            return Err(io::Error::other("stop capture before staging final tails"));
        }
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut entries = Vec::new();
        for (id, attachment) in self
            .attachments
            .range_mut((lower, Bound::Unbounded))
            .take(limit.get())
        {
            for _ in 0..self.config.event_batch.get() {
                if !attachment.stage_next()? {
                    break;
                }
            }
            let staged = attachment.dispatch.cursor().source_position();
            let completion = CollectorFinished::read(
                &self
                    .config
                    .directory
                    .join("collectors")
                    .join(id.to_string()),
                *id,
            )?;
            let state = match completion {
                None => CaptureTailState::AwaitingCompletion,
                Some(completed) if staged == completed.stdout_position => {
                    CaptureTailState::Staged {
                        position: staged,
                        pending_forwarding: attachment.dispatch.lanes().next().is_some(),
                    }
                }
                Some(completed) => {
                    let target = completed.stdout_position;
                    if staged.byte_offset > target.byte_offset
                        || staged.next_sequence > target.next_sequence
                    {
                        return Err(io::Error::other(
                            "extraction cursor exceeds collector completion",
                        ));
                    }
                    CaptureTailState::Pending { staged, target }
                }
            };
            entries.push(CaptureTailEntry {
                attachment_id: *id,
                state,
            });
        }
        let next_cursor = entries.last().and_then(|entry| {
            self.attachments
                .range((Bound::Excluded(entry.attachment_id), Bound::Unbounded))
                .next()
                .map(|_| entry.attachment_id)
        });
        Ok(CaptureTailPage {
            entries,
            next_cursor,
        })
    }
}

impl Attachment {
    pub(super) fn stage_next(&mut self) -> io::Result<bool> {
        let staged = self.dispatch.cursor();
        // A failed stage may already have consumed a source batch in memory.
        // Restart decoding at the durable dispatch cursor before the next try;
        // partially enqueued frames retain their original idempotency keys.
        match self.reader.cursor() {
            Ok(cursor) if cursor.source_position() == staged.source_position() => {}
            Ok(_) | Err(_) => {
                self.reader = ControlFrameReader::open(&self.source, staged)?;
            }
        }
        let Some(batch) = self.reader.next_batch()? else {
            return Ok(false);
        };
        self.dispatch.stage(batch, self.reader.cursor()?)?;
        Ok(true)
    }
}
