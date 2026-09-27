use std::io;
use std::num::NonZeroUsize;

use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;

use super::StoreAudit;
use super::StoreAuditEvent;

/// One raw store operation record, before history filtering or compaction.
pub struct StoreAuditRecord {
    pub sequence: u64,
    pub event: StoreAuditEvent,
}

/// A bounded page within the durable prefix sampled at the start of the read.
/// Persist `next` only after consuming every record. Reaching `durable_end`
/// means caught up with this sample, not that the live producer has stopped.
pub struct StoreAuditPage {
    pub records: Vec<StoreAuditRecord>,
    pub next: JournalPosition,
    pub durable_end: JournalPosition,
}

impl StoreAudit {
    /// Reads raw evidence without requiring all store operations to settle.
    /// Started records alone do not prove a write succeeded; consumers must
    /// match Finished records and the store's persistence boundary separately.
    /// The limit counts journal records, whose payloads retain full store writes.
    pub async fn read_page(
        &self,
        cursor: JournalPosition,
        limit: NonZeroUsize,
    ) -> io::Result<StoreAuditPage> {
        let audit = self.clone();
        tokio::task::spawn_blocking(move || {
            let durable_end = {
                let writer = audit
                    .writer
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                if let Some(error) = &writer.failure {
                    return Err(io::Error::other(error.clone()));
                }
                writer.journal.position()
            };
            if cursor.next_sequence > durable_end.next_sequence
                || cursor.byte_offset > durable_end.byte_offset
                || (cursor.next_sequence == durable_end.next_sequence
                    && cursor.byte_offset != durable_end.byte_offset)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "store audit cursor exceeds durable prefix",
                ));
            }
            let mut reader = JournalReader::open(&audit.path, cursor)?;
            let mut records = Vec::new();
            while reader.position().next_sequence < durable_end.next_sequence
                && records.len() < limit.get()
            {
                let record = reader.next_record()?.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "store audit durable prefix is incomplete",
                    )
                })?;
                if reader.position().byte_offset > durable_end.byte_offset {
                    return Err(io::Error::other("store audit frame exceeds durable prefix"));
                }
                let event: StoreAuditEvent = serde_json::from_slice(&record.payload)?;
                if let StoreAuditEvent::Opened { identity } = &event
                    && identity != &audit.identity
                {
                    return Err(io::Error::other("store audit page identity changed"));
                }
                records.push(StoreAuditRecord {
                    sequence: record.sequence,
                    event,
                });
            }
            let next = reader.position();
            if next.next_sequence == durable_end.next_sequence
                && next.byte_offset != durable_end.byte_offset
            {
                return Err(io::Error::other("store audit durable boundary changed"));
            }
            Ok(StoreAuditPage {
                records,
                next,
                durable_end,
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}
