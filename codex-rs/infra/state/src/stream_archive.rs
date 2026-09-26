use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::path::Path;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use serde::Deserialize;
use serde::Serialize;

use crate::ArchiveReceipt;
use crate::Journal;
use crate::JournalPosition;
use crate::SessionShard;

/// Identifies one append-only producer journal across archive and restoration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveStream {
    pub root_session_id: RootSessionId,
    pub machine_id: MachineId,
    pub agent_id: AgentId,
    pub launch_id: MessageId,
    pub name: String,
}

/// Raw journal bytes may split a record across segments. Concatenation restores
/// the original frames, including their sequence numbers and checksums.
#[derive(Debug, Serialize, Deserialize)]
pub struct JournalSegment {
    pub stream: ArchiveStream,
    pub start: u64,
    pub end: u64,
    pub durable: JournalPosition,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedRange {
    pub start: u64,
    pub end: u64,
    pub durable: JournalPosition,
    pub receipt: ArchiveReceipt,
}

/// Producer completion, separate from periodic archive snapshots. The receipt
/// identifies the commit that contains the final contiguous segment prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamCompletion {
    pub stream: ArchiveStream,
    pub position: JournalPosition,
    pub archive: ArchiveReceipt,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ArchiveEvent {
    Opened { stream: ArchiveStream },
    Published { range: ArchivedRange },
    CompletionRequested { completion: StreamCompletion },
    Completed { receipt: ArchiveReceipt },
}

/// Tracks a contiguous remotely confirmed prefix. The machine writer supplies
/// its producer's fsync-acknowledged position, never an observed file length.
/// Call off the async executor; Git publication and receipt recording block.
pub struct JournalArchive {
    stream: ArchiveStream,
    journal: Journal,
    latest: Option<ArchivedRange>,
    completion: Option<StreamCompletion>,
    completion_receipt: Option<ArchiveReceipt>,
}

impl JournalArchive {
    pub fn open(path: &Path, stream: ArchiveStream) -> io::Result<Self> {
        let mut latest = None;
        let mut opened = false;
        let mut completion = None;
        let mut completion_receipt = None;
        let mut journal = Journal::open(path, |record| {
            match serde_json::from_slice(&record.payload).map_err(io::Error::other)? {
                ArchiveEvent::Opened { stream: previous } => {
                    if opened || previous != stream {
                        return Err(io::Error::other("archive stream binding changed"));
                    }
                    opened = true;
                }
                ArchiveEvent::Published { range } => {
                    if !opened || completion.is_some() {
                        return Err(io::Error::other("archive receipt has no stream binding"));
                    }
                    validate_range(&stream, latest.as_ref(), &range)?;
                    latest = Some(range);
                }
                ArchiveEvent::CompletionRequested {
                    completion: requested,
                } => {
                    let range = latest.as_ref().ok_or_else(|| {
                        io::Error::other("stream completion has no archived prefix")
                    })?;
                    if completion.is_some()
                        || requested.stream != stream
                        || range.end != requested.position.byte_offset
                        || range.durable != requested.position
                        || range.receipt != requested.archive
                    {
                        return Err(io::Error::other(
                            "stream completion differs from archived prefix",
                        ));
                    }
                    completion = Some(requested);
                }
                ArchiveEvent::Completed { receipt } => {
                    let requested = completion
                        .as_ref()
                        .ok_or_else(|| io::Error::other("completion receipt has no request"))?;
                    if completion_receipt.is_some()
                        || receipt.session_ref != requested.archive.session_ref
                    {
                        return Err(io::Error::other("completion receipt binding changed"));
                    }
                    completion_receipt = Some(receipt);
                }
            }
            Ok(())
        })?;
        if !opened {
            journal.append(
                &serde_json::to_vec(&ArchiveEvent::Opened {
                    stream: stream.clone(),
                })
                .map_err(io::Error::other)?,
            )?;
        }
        Ok(Self {
            stream,
            journal,
            latest,
            completion,
            completion_receipt,
        })
    }

    pub fn latest(&self) -> Option<&ArchivedRange> {
        self.latest.as_ref()
    }

    /// Call after the producer has stopped. The requested boundary must equal
    /// the fully archived observed position. Once recorded, this stream admits
    /// only completion retries; new producer work needs its own stream binding.
    pub fn seal(
        &mut self,
        required: JournalPosition,
        shard: &mut SessionShard,
    ) -> io::Result<ArchiveReceipt> {
        let range = self.require_archived(required)?;
        if range.end != required.byte_offset || range.durable != required {
            return Err(io::Error::other(
                "completion position differs from final archived position",
            ));
        }
        let requested = StreamCompletion {
            stream: self.stream.clone(),
            position: required,
            archive: range.receipt.clone(),
        };
        match &self.completion {
            Some(previous) if previous != &requested => {
                return Err(io::Error::other("stream completion changed"));
            }
            Some(_) => {}
            None => {
                self.journal.append(
                    &serde_json::to_vec(&ArchiveEvent::CompletionRequested {
                        completion: requested.clone(),
                    })
                    .map_err(io::Error::other)?,
                )?;
                self.completion = Some(requested.clone());
            }
        }
        if let Some(receipt) = &self.completion_receipt {
            return Ok(receipt.clone());
        }
        let receipt = shard.publish_completion(&requested)?;
        self.journal.append(
            &serde_json::to_vec(&ArchiveEvent::Completed {
                receipt: receipt.clone(),
            })
            .map_err(io::Error::other)?,
        )?;
        self.completion_receipt = Some(receipt.clone());
        Ok(receipt)
    }

    /// Checks a producer-supplied completion position against the contiguous
    /// remote prefix. A segment's observed durable position alone is not proof
    /// that all bytes up to that position have been archived.
    pub fn require_archived(&self, required: JournalPosition) -> io::Result<&ArchivedRange> {
        self.latest
            .as_ref()
            .filter(|range| {
                range.end >= required.byte_offset
                    && range.durable.next_sequence >= required.next_sequence
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "producer completion position has not been remotely archived",
                )
            })
    }

    /// Publishes at most `max_bytes` raw bytes, including when one journal
    /// record exceeds the segment budget. Only a confirmed remote push advances
    /// the cursor. An interrupted receipt append republishes an overlapping
    /// immutable range; restoration uses offsets rather than concatenating all
    /// blobs indiscriminately.
    pub fn publish_next(
        &mut self,
        source: &Path,
        durable: JournalPosition,
        max_bytes: NonZeroUsize,
        shard: &mut SessionShard,
    ) -> io::Result<Option<ArchivedRange>> {
        if self.completion.is_some() {
            return Err(io::Error::other(
                "archive stream is completing or completed",
            ));
        }
        let start = self.latest.as_ref().map_or(0, |range| range.end);
        if self.latest.as_ref().is_some_and(|range| {
            durable.byte_offset < range.durable.byte_offset
                || durable.next_sequence < range.durable.next_sequence
        }) || durable.byte_offset < start
        {
            return Err(io::Error::other(
                "producer durable position moved backwards",
            ));
        }
        if durable.byte_offset == start && self.latest.is_some() {
            return Ok(None);
        }
        let expected_ref = format!(
            "refs/codex/session-shards/{}/{}",
            self.stream.root_session_id, self.stream.machine_id
        );
        if shard.session_ref() != expected_ref {
            return Err(io::Error::other(
                "archive shard differs from producer machine",
            ));
        }
        let length = (durable.byte_offset - start).min(max_bytes.get() as u64);
        let mut file = File::open(source)?;
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = vec![0; usize::try_from(length).map_err(io::Error::other)?];
        file.read_exact(&mut bytes)?;
        let segment = JournalSegment {
            stream: self.stream.clone(),
            start,
            end: start + length,
            durable,
            bytes,
        };
        let receipt = shard.publish_journal_segment(&segment)?;
        let range = ArchivedRange {
            start,
            end: segment.end,
            durable,
            receipt,
        };
        validate_range(&self.stream, self.latest.as_ref(), &range)?;
        self.journal.append(
            &serde_json::to_vec(&ArchiveEvent::Published {
                range: range.clone(),
            })
            .map_err(io::Error::other)?,
        )?;
        self.latest = Some(range.clone());
        Ok(Some(range))
    }
}

fn validate_range(
    stream: &ArchiveStream,
    previous: Option<&ArchivedRange>,
    range: &ArchivedRange,
) -> io::Result<()> {
    let expected_start = previous.map_or(0, |previous| previous.end);
    let expected_ref = format!(
        "refs/codex/session-shards/{}/{}",
        stream.root_session_id, stream.machine_id
    );
    if range.start != expected_start
        || range.end < range.start
        || (range.end == range.start
            && (previous.is_some()
                || range.start != 0
                || range.durable != JournalPosition::default()))
        || range.end > range.durable.byte_offset
        || range.receipt.session_ref != expected_ref
        || previous.is_some_and(|previous| {
            range.durable.byte_offset < previous.durable.byte_offset
                || range.durable.next_sequence < previous.durable.next_sequence
        })
    {
        return Err(io::Error::other(
            "archive receipt does not extend the confirmed stream prefix",
        ));
    }
    Ok(())
}
