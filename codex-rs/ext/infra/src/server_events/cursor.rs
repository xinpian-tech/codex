use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;

use super::AgentServerEventPage;
use super::AgentServerEvents;

mod archive;

/// Processing progress for one event source and one app-server instance.
/// Acknowledging a page records that its consumer finished handling it; it does
/// not itself handle server requests or prove their effects were applied.
#[derive(Clone)]
pub struct AgentEventCursor {
    writer: Arc<Mutex<Writer>>,
}

struct Writer {
    path: PathBuf,
    source: PathBuf,
    run_start: JournalPosition,
    position: JournalPosition,
    journal: Journal,
    closed: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event {
    Opened {
        source: PathBuf,
        run_start: JournalPosition,
    },
    Advanced {
        start: JournalPosition,
        next: JournalPosition,
    },
}

impl AgentServerEvents {
    /// Each runtime gets a separate cursor journal beside its event source.
    /// Old progress remains auditable without making old requests live again.
    pub async fn open_cursor(&self) -> io::Result<AgentEventCursor> {
        let source = self.path.clone();
        let run_start = self.run_start;
        let path = source.with_file_name(format!(
            "server-events-cursor-{}.journal",
            run_start.next_sequence
        ));
        tokio::task::spawn_blocking(move || {
            let mut opened = false;
            let mut position = run_start;
            let mut journal = Journal::open(&path, |record| {
                match serde_json::from_slice::<Event>(&record.payload)? {
                    Event::Opened {
                        source: previous,
                        run_start: previous_start,
                    } => {
                        if opened || previous != source || previous_start != run_start {
                            return Err(io::Error::other("event cursor source binding changed"));
                        }
                        opened = true;
                    }
                    Event::Advanced { start, next } => {
                        if !opened
                            || start != position
                            || next.next_sequence <= start.next_sequence
                            || next.byte_offset <= start.byte_offset
                        {
                            return Err(io::Error::other(
                                "event cursor progress is not contiguous",
                            ));
                        }
                        position = next;
                    }
                }
                Ok(())
            })?;
            if !opened {
                journal.append(&serde_json::to_vec(&Event::Opened {
                    source: source.clone(),
                    run_start,
                })?)?;
            }
            Ok(AgentEventCursor {
                writer: Arc::new(Mutex::new(Writer {
                    path: path.canonicalize()?,
                    source,
                    run_start,
                    position,
                    journal,
                    closed: false,
                })),
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}

impl AgentEventCursor {
    pub async fn read_page(
        &self,
        events: &AgentServerEvents,
        limit: NonZeroUsize,
    ) -> io::Result<AgentServerEventPage> {
        let position = {
            let writer = self
                .writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            if writer.closed || writer.source != events.path || writer.run_start != events.run_start
            {
                return Err(io::Error::other(
                    "event cursor is closed or belongs to another runtime",
                ));
            }
            writer.position
        };
        events.read_page(position, limit).await
    }

    /// Call only after every event in this page has been handled. Cancellation
    /// does not undo the owned journal write; retrying this exact page is safe.
    pub async fn acknowledge(&self, page: &AgentServerEventPage) -> io::Result<()> {
        let start = page.start;
        let next = page.next;
        if next.next_sequence < start.next_sequence
            || next.byte_offset < start.byte_offset
            || next.next_sequence > page.durable_end.next_sequence
            || next.byte_offset > page.durable_end.byte_offset
            || next.next_sequence - start.next_sequence != page.records.len() as u64
        {
            return Err(io::Error::other("event page progress is inconsistent"));
        }
        let run_start = page.run_start;
        let source = page.source.clone();
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let mut writer = writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            if writer.closed || writer.run_start != run_start || writer.source != source {
                return Err(io::Error::other(
                    "event cursor cannot acknowledge this runtime",
                ));
            }
            if writer.position == next {
                return Ok(());
            }
            if writer.position != start
                || next.next_sequence <= start.next_sequence
                || next.byte_offset <= start.byte_offset
            {
                return Err(io::Error::other(
                    "event cursor acknowledgement is out of order",
                ));
            }
            writer
                .journal
                .append(&serde_json::to_vec(&Event::Advanced { start, next })?)?;
            writer.position = next;
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Stop processing before closing, then archive this exact final prefix.
    pub async fn close(&self) -> io::Result<(PathBuf, JournalPosition)> {
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let mut writer = writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            writer.closed = true;
            Ok((writer.path.clone(), writer.journal.position()))
        })
        .await
        .map_err(io::Error::other)?
    }
}
