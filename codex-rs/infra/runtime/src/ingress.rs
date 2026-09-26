use std::io;
use std::path::Path;

use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use codex_infra_state::JournalRecord;

use crate::GatewayInbox;
use crate::ReceptionEvent;

/// Records network arrivals before handing frames to per-recipient queues. A
/// separate durable cursor lets restart replay the interval between both writes.
pub struct IngressSpool {
    events: Journal,
    reader: JournalReader,
    checkpoints: Journal,
    pending: Option<JournalRecord>,
}

impl IngressSpool {
    pub fn open(event_path: &Path, checkpoint_path: &Path) -> io::Result<Self> {
        let events = Journal::open(event_path, |record| {
            let _: ReceptionEvent =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            Ok(())
        })?;
        let mut position = JournalPosition::default();
        let checkpoints = Journal::open(checkpoint_path, |record| {
            let next: JournalPosition =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            if position.next_sequence.checked_add(1) != Some(next.next_sequence)
                || next.byte_offset <= position.byte_offset
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ingress cursor does not continue previous record",
                ));
            }
            position = next;
            Ok(())
        })?;
        if position.next_sequence > events.next_sequence()
            || position.byte_offset > events.position().byte_offset
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ingress cursor exceeds event journal",
            ));
        }
        let reader = JournalReader::open(event_path, position)?;
        Ok(Self {
            events,
            reader,
            checkpoints,
            pending: None,
        })
    }

    pub fn record(&mut self, event: &ReceptionEvent) -> io::Result<u64> {
        self.events
            .append(&serde_json::to_vec(event).map_err(io::Error::other)?)
    }

    /// Each call handles one event. Errors retain its position for retry; a
    /// repeated frame enqueue uses the same connection/sequence queue key.
    pub fn advance_one(&mut self, inbox: &mut GatewayInbox) -> io::Result<Option<ReceptionEvent>> {
        if self.pending.is_none() {
            self.pending = self.reader.next_record()?;
        }
        let Some(record) = &self.pending else {
            return Ok(None);
        };
        let event: ReceptionEvent =
            serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
        match &event {
            ReceptionEvent::Frame {
                connection_id,
                sequence,
                frame,
            } => {
                inbox.stage(*connection_id, *sequence, frame)?;
            }
            ReceptionEvent::Opened { .. }
            | ReceptionEvent::Closed { .. }
            | ReceptionEvent::ListenerFailed { .. }
            | ReceptionEvent::WorkerFailed { .. } => {}
        }
        self.checkpoints
            .append(&serde_json::to_vec(&self.reader.position()).map_err(io::Error::other)?)?;
        self.pending = None;
        Ok(Some(event))
    }
}
