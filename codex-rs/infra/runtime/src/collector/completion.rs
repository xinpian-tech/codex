use std::io;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use serde::Deserialize;
use serde::Serialize;

/// Durable completion of one control attachment, not of its panes or Session.
/// Written only after child exit, both capture workers and the lifecycle append.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorFinished {
    pub attachment_id: MessageId,
    pub exit_status: String,
    pub exit_code: Option<i32>,
    pub success: bool,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_position: JournalPosition,
    pub stderr_position: JournalPosition,
    pub commands_position: JournalPosition,
    pub lifecycle_position: JournalPosition,
}

impl CollectorFinished {
    pub(super) fn persist(&self, directory: &Path) -> io::Result<()> {
        let mut existing = None;
        let mut journal = Journal::open(&directory.join("completion.journal"), |record| {
            if existing.is_some() {
                return Err(io::Error::other("duplicate collector completion"));
            }
            existing = Some(serde_json::from_slice::<Self>(&record.payload)?);
            Ok(())
        })?;
        match existing {
            Some(completed) if completed == *self => Ok(()),
            Some(_) => Err(io::Error::other("collector completion changed")),
            None => journal.append(&serde_json::to_vec(self)?).map(|_| ()),
        }
    }

    /// Reads the immutable completion marker without opening any stream writer.
    /// Missing or incomplete markers remain unfinished; file lengths are never
    /// substituted for producer acknowledgments. The caller supplies the known
    /// attachment identity from the machine's collector directory.
    pub fn read(directory: &Path, attachment_id: MessageId) -> io::Result<Option<Self>> {
        Self::read_with_position(directory, attachment_id)
            .map(|record| record.map(|(finished, _)| finished))
    }

    pub(super) fn read_with_position(
        directory: &Path,
        attachment_id: MessageId,
    ) -> io::Result<Option<(Self, JournalPosition)>> {
        let mut reader = match JournalReader::open(
            &directory.join("completion.journal"),
            JournalPosition::default(),
        ) {
            Ok(reader) => reader,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(record) = reader.next_record()? else {
            return Ok(None);
        };
        let finished: Self = serde_json::from_slice(&record.payload)?;
        if finished.attachment_id != attachment_id || reader.next_record()?.is_some() {
            return Err(io::Error::other(
                "collector completion identity or count mismatch",
            ));
        }
        Ok(Some((finished, reader.position())))
    }
}
