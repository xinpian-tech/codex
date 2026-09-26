use std::io;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use serde::Deserialize;
use serde::Serialize;

use super::ControlDispatch;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DispatchCompletion {
    pub(crate) attachment_id: MessageId,
    pub(crate) queue: JournalPosition,
    pub(crate) cursor: JournalPosition,
}

impl ControlDispatch {
    pub(crate) fn positions(&self) -> DispatchCompletion {
        DispatchCompletion {
            attachment_id: self.attachment_id,
            queue: self.queue.position(),
            cursor: self.checkpoints.position(),
        }
    }

    /// The owner has established collector EOF, final cursor and an empty
    /// dispatch queue, and removes this writer immediately after this succeeds.
    pub(crate) fn record_retirement(&self, path: &Path) -> io::Result<()> {
        let expected = self.positions();
        let mut saved = false;
        let mut journal = Journal::open(path, |record| {
            let previous: DispatchCompletion = serde_json::from_slice(&record.payload)?;
            if saved || previous != expected {
                return Err(io::Error::other("dispatch retirement changed"));
            }
            saved = true;
            Ok(())
        })?;
        if !saved {
            journal.append(&serde_json::to_vec(&expected)?)?;
        }
        Ok(())
    }
}

impl DispatchCompletion {
    pub(crate) fn read(
        path: &Path,
        attachment_id: MessageId,
    ) -> io::Result<Option<(Self, JournalPosition)>> {
        let mut reader = match JournalReader::open(path, JournalPosition::default()) {
            Ok(reader) => reader,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(record) = reader.next_record()? else {
            return Ok(None);
        };
        let completed: Self = serde_json::from_slice(&record.payload)?;
        if completed.attachment_id != attachment_id || reader.next_record()?.is_some() {
            return Err(io::Error::other(
                "dispatch retirement identity or count mismatch",
            ));
        }
        Ok(Some((completed, reader.position())))
    }
}
