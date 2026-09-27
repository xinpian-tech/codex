use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_state::ArchiveStream;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use serde::Deserialize;
use serde::Serialize;

use crate::AccountDirectory;
use crate::AccountDirectoryUpdate;

/// The immutable journal prefix restored from one machine archive stream.
/// Further restores may extend this same file; the stream identity stays fixed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountDirectorySource {
    pub stream: ArchiveStream,
    pub path: PathBuf,
}

#[derive(Clone, Copy, Debug)]
pub struct AccountDirectoryProgress {
    pub position: JournalPosition,
    pub applied_records: usize,
    pub caught_up: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event {
    Opened {
        source: AccountDirectorySource,
        target: PathBuf,
    },
    Advanced {
        position: JournalPosition,
    },
}

struct State {
    journal: Journal,
    source: AccountDirectorySource,
    target: AccountDirectory,
    position: JournalPosition,
}

/// Maintains one Agent's account directory from its machine's merged stream.
/// This follower is the replica's only update writer; clients only subscribe.
/// Its own journal serializes replay, so a crash between target persistence and
/// cursor persistence repeats only the last already-applied head event.
#[derive(Clone)]
pub struct AccountDirectoryFollower {
    state: Arc<Mutex<State>>,
}

impl AccountDirectoryFollower {
    /// Blocking preparation. Both source and target journals already exist;
    /// the caller creates the cursor journal's parent directory.
    pub fn open(
        mut source: AccountDirectorySource,
        target: AccountDirectory,
        cursor: &Path,
    ) -> io::Result<Self> {
        source.path = source.path.canonicalize()?;
        let (target_path, _) = target.archive_snapshot()?;
        if source.path == target_path || source.stream.name != "account-directory" {
            return Err(io::Error::other(
                "account directory follower source differs",
            ));
        }
        let mut opened = false;
        let mut position = JournalPosition::default();
        let mut journal = Journal::open(cursor, |record| {
            match serde_json::from_slice(&record.payload)? {
                Event::Opened {
                    source: saved,
                    target: saved_target,
                } => {
                    if opened || saved != source || saved_target != target_path {
                        return Err(io::Error::other(
                            "account directory follower binding changed",
                        ));
                    }
                    opened = true;
                }
                Event::Advanced { position: next } => {
                    if !opened
                        || next.next_sequence
                            != position.next_sequence.checked_add(1).ok_or_else(|| {
                                io::Error::other("account directory cursor exhausted")
                            })?
                        || next.byte_offset <= position.byte_offset
                    {
                        return Err(io::Error::other(
                            "account directory follower cursor differs",
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
                target: target_path,
            })?)?;
        }
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                journal,
                source,
                target,
                position,
            })),
        })
    }

    /// Advances at most `batch` records, never beyond the producer-confirmed or
    /// verified-restore position. An owned worker finishes persistence even when
    /// the waiter is canceled. Missing predecessors remain at the current cursor.
    pub async fn advance(
        &self,
        durable: JournalPosition,
        batch: NonZeroUsize,
    ) -> io::Result<AccountDirectoryProgress> {
        let follower = self.clone();
        tokio::task::spawn_blocking(move || follower.advance_blocking(durable, batch))
            .await
            .map_err(io::Error::other)?
    }

    pub(super) fn advance_blocking(
        &self,
        durable: JournalPosition,
        batch: NonZeroUsize,
    ) -> io::Result<AccountDirectoryProgress> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if durable.next_sequence < state.position.next_sequence
            || durable.byte_offset < state.position.byte_offset
        {
            return Err(io::Error::other(
                "account directory source prefix regressed",
            ));
        }
        let mut reader = JournalReader::open(&state.source.path, state.position)?;
        let mut applied_records = 0;
        while state.position != durable && applied_records < batch.get() {
            let record = reader.next_record()?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "account directory prefix is incomplete",
                )
            })?;
            let next = reader.position();
            if next.next_sequence > durable.next_sequence || next.byte_offset > durable.byte_offset
            {
                return Err(io::Error::other(
                    "account directory record exceeds confirmed prefix",
                ));
            }
            let update: AccountDirectoryUpdate = serde_json::from_slice(&record.payload)?;
            state.target.apply_update(update)?;
            state
                .journal
                .append(&serde_json::to_vec(&Event::Advanced { position: next })?)?;
            state.position = next;
            applied_records += 1;
        }
        Ok(AccountDirectoryProgress {
            position: state.position,
            applied_records,
            caught_up: state.position == durable,
        })
    }
}
