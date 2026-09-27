use std::io;
use std::num::NonZeroUsize;

use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;

use super::AgentServerEvent;
use super::AgentServerEvents;

pub struct AgentServerEventRecord {
    pub position: JournalPosition,
    pub event: AgentServerEvent,
}

/// A page in this app-server instance's durable event prefix. Persist processing
/// progress only after handling the page; the run_start identifies its owner.
pub struct AgentServerEventPage {
    pub run_start: JournalPosition,
    pub start: JournalPosition,
    pub next: JournalPosition,
    pub durable_end: JournalPosition,
    pub records: Vec<AgentServerEventRecord>,
}

impl AgentServerEvents {
    /// Reads current-run events without borrowing or blocking the capture task.
    /// Historical runs are retained in source() but cannot feed this live RPC
    /// consumer. A new consumer starts at run_start(), not sequence zero.
    pub async fn read_page(
        &self,
        cursor: JournalPosition,
        limit: NonZeroUsize,
    ) -> io::Result<AgentServerEventPage> {
        let (path, durable_end) = self.snapshot()?;
        let run_start = self.run_start;
        if cursor.next_sequence < run_start.next_sequence
            || cursor.byte_offset < run_start.byte_offset
            || cursor.next_sequence > durable_end.next_sequence
            || cursor.byte_offset > durable_end.byte_offset
            || (cursor.next_sequence == run_start.next_sequence && cursor != run_start)
            || (cursor.next_sequence == durable_end.next_sequence && cursor != durable_end)
        {
            return Err(io::Error::other(
                "server event cursor is outside the current run",
            ));
        }
        tokio::task::spawn_blocking(move || {
            let mut reader = JournalReader::open(&path, cursor)?;
            let mut records = Vec::new();
            while reader.position().next_sequence < durable_end.next_sequence
                && records.len() < limit.get()
            {
                let record = reader.next_record()?.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "server event durable prefix is incomplete",
                    )
                })?;
                if reader.position().byte_offset > durable_end.byte_offset {
                    return Err(io::Error::other("server event exceeds sampled prefix"));
                }
                records.push(AgentServerEventRecord {
                    position: record.position,
                    event: serde_json::from_slice(&record.payload)?,
                });
            }
            let next = reader.position();
            if next.next_sequence == durable_end.next_sequence && next != durable_end {
                return Err(io::Error::other("server event durable boundary changed"));
            }
            Ok(AgentServerEventPage {
                run_start,
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
