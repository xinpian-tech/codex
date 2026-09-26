use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::Journal;
use crate::JournalPosition;
use crate::JournalReader;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueItem {
    pub key: String,
    pub lane: String,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum QueueEvent {
    Enqueued(QueueItem),
    Completed { key: String },
}

struct Entry {
    position: JournalPosition,
    lane: String,
    completed: bool,
}

type Pending = BTreeMap<String, BTreeMap<u64, String>>;

/// Payloads stay in the append-only journal; memory contains keys and offsets.
/// Independent lanes let a slow recipient retain its backlog without stopping
/// the consumers servicing other destinations.
pub struct SpoolQueue {
    journal: Journal,
    path: PathBuf,
    entries: BTreeMap<String, Entry>,
    pending: Pending,
}

impl SpoolQueue {
    /// Includes durable intents and completions; pending work remains pending.
    pub fn position(&self) -> JournalPosition {
        self.journal.position()
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let mut entries = BTreeMap::new();
        let mut pending = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            let event = serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            apply(&mut entries, &mut pending, event, record.position)
        })?;
        Ok(Self {
            journal,
            path: path.canonicalize()?,
            entries,
            pending,
        })
    }

    pub fn enqueue(&mut self, item: QueueItem) -> io::Result<u64> {
        if let Some(entry) = self.entries.get(&item.key) {
            let previous = self.read(&item.key)?;
            return if previous == item {
                Ok(entry.position.next_sequence)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "queue key reused with different content",
                ))
            };
        }
        self.append(QueueEvent::Enqueued(item))
    }

    pub fn read(&self, key: &str) -> io::Result<QueueItem> {
        let entry = self
            .entries
            .get(key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "queue key missing"))?;
        let record = JournalReader::open(&self.path, entry.position)?
            .next_record()?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "queued payload missing")
            })?;
        match serde_json::from_slice(&record.payload).map_err(io::Error::other)? {
            QueueEvent::Enqueued(item) => Ok(item),
            QueueEvent::Completed { .. } => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "queue offset points to completion",
            )),
        }
    }

    pub fn pending_keys(&self, lane: &str, first_sequence: u64, limit: NonZeroUsize) -> Vec<&str> {
        self.pending
            .get(lane)
            .into_iter()
            .flat_map(|entries| entries.range(first_sequence..))
            .take(limit.get())
            .map(|(_, key)| key.as_str())
            .collect()
    }

    pub fn lanes(&self) -> impl Iterator<Item = &str> {
        self.pending.keys().map(String::as_str)
    }

    /// Queue completion is consumer-specific. For network frames it records a
    /// forwarding attempt, while the host outbox still awaits the inbox receipt.
    pub fn complete(&mut self, key: &str) -> io::Result<()> {
        let entry = self
            .entries
            .get(key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "queue key missing"))?;
        if !entry.completed {
            self.append(QueueEvent::Completed {
                key: key.to_owned(),
            })?;
        }
        Ok(())
    }

    fn append(&mut self, event: QueueEvent) -> io::Result<u64> {
        let position = self.journal.position();
        let sequence = self
            .journal
            .append(&serde_json::to_vec(&event).map_err(io::Error::other)?)?;
        apply(&mut self.entries, &mut self.pending, event, position)?;
        Ok(sequence)
    }
}

fn apply(
    entries: &mut BTreeMap<String, Entry>,
    pending: &mut Pending,
    event: QueueEvent,
    position: JournalPosition,
) -> io::Result<()> {
    match event {
        QueueEvent::Enqueued(item) => {
            if entries.contains_key(&item.key) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate queue enqueue",
                ));
            }
            pending
                .entry(item.lane.clone())
                .or_default()
                .insert(position.next_sequence, item.key.clone());
            entries.insert(
                item.key,
                Entry {
                    position,
                    lane: item.lane,
                    completed: false,
                },
            );
        }
        QueueEvent::Completed { key } => {
            let entry = entries.get_mut(&key).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "queue completion without enqueue",
                )
            })?;
            entry.completed = true;
            if let Some(lane) = pending.get_mut(&entry.lane) {
                lane.remove(&entry.position.next_sequence);
                if lane.is_empty() {
                    pending.remove(&entry.lane);
                }
            }
        }
    }
    Ok(())
}
