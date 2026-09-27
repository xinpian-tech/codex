use std::io;
use std::num::NonZeroUsize;

use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;

use super::ModelInputAudit;
use super::ModelInputAuditEvent;

pub struct ModelInputAuditRecord {
    pub sequence: u64,
    pub event: ModelInputAuditEvent,
}

/// A bounded number of full audit records from a sampled durable prefix.
/// Advance a persisted cursor only after consuming the entire page.
pub struct ModelInputAuditPage {
    pub start: JournalPosition,
    pub next: JournalPosition,
    pub durable_end: JournalPosition,
    pub records: Vec<ModelInputAuditRecord>,
}

impl ModelInputAudit {
    pub async fn read_page(
        &self,
        cursor: JournalPosition,
        limit: NonZeroUsize,
    ) -> io::Result<ModelInputAuditPage> {
        let audit = self.clone();
        tokio::task::spawn_blocking(move || {
            let (path, durable_end) = audit.snapshot()?;
            if cursor.next_sequence > durable_end.next_sequence
                || cursor.byte_offset > durable_end.byte_offset
                || (cursor.next_sequence == durable_end.next_sequence
                    && cursor.byte_offset != durable_end.byte_offset)
            {
                return Err(io::Error::other(
                    "model input cursor exceeds durable prefix",
                ));
            }
            let mut reader = JournalReader::open(&path, cursor)?;
            let mut records = Vec::new();
            while records.len() < limit.get()
                && reader.position().next_sequence < durable_end.next_sequence
            {
                let record = reader.next_record()?.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "model input durable prefix is incomplete",
                    )
                })?;
                if reader.position().byte_offset > durable_end.byte_offset {
                    return Err(io::Error::other(
                        "model input record exceeds durable prefix",
                    ));
                }
                records.push(ModelInputAuditRecord {
                    sequence: record.sequence,
                    event: serde_json::from_slice(&record.payload)?,
                });
            }
            let next = reader.position();
            if next.next_sequence == durable_end.next_sequence
                && next.byte_offset != durable_end.byte_offset
            {
                return Err(io::Error::other("model input durable boundary changed"));
            }
            Ok(ModelInputAuditPage {
                start: cursor,
                next,
                durable_end,
                records,
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}
