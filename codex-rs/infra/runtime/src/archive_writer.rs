use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_protocol::MessageId;
use codex_infra_state::ArchiveReceipt;
use codex_infra_state::ArchiveStream;
use codex_infra_state::JournalArchive;
use codex_infra_state::JournalPosition;
use codex_infra_state::QueueItem;
use codex_infra_state::SessionShard;
use codex_infra_state::SpoolQueue;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "position", rename_all = "snake_case")]
pub enum ArchiveTarget {
    Snapshot(JournalPosition),
    ProducerFinished(JournalPosition),
}

/// Host-local machine work. Producer positions come from writer acknowledgments
/// or completion records; paths are not instructions received from the model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveJob {
    pub job_id: MessageId,
    pub stream: ArchiveStream,
    pub source: PathBuf,
    pub receipt_journal: PathBuf,
    pub target: ArchiveTarget,
}

pub enum ArchiveAdvance {
    Pending {
        job_id: MessageId,
    },
    Completed {
        job_id: MessageId,
        receipt: ArchiveReceipt,
    },
}

/// Owns the machine's Session Git writer and durable jobs. Each call advances
/// one chunk from one stream, rotating lanes so a growing stream does not
/// monopolize the writer. Run these blocking operations off the async executor.
pub struct MachineArchiveWriter {
    shard: SessionShard,
    jobs: SpoolQueue,
    last_lane: Option<String>,
    chunk_bytes: NonZeroUsize,
    streams: BTreeMap<String, ActiveArchive>,
}

struct ActiveArchive {
    source: PathBuf,
    receipt_journal: PathBuf,
    archive: JournalArchive,
}

impl MachineArchiveWriter {
    pub fn open(shard: SessionShard, queue: &Path, chunk_bytes: NonZeroUsize) -> io::Result<Self> {
        Ok(Self {
            shard,
            jobs: SpoolQueue::open(queue)?,
            last_lane: None,
            chunk_bytes,
            streams: BTreeMap::new(),
        })
    }

    /// Reuse the same job ID and payload to retry admission. Requests for one
    /// stream are FIFO; its producer-finished request follows all snapshots.
    pub fn enqueue(&mut self, job: ArchiveJob) -> io::Result<u64> {
        if !job.source.is_absolute() || !job.receipt_journal.is_absolute() {
            return Err(io::Error::other(
                "archive job paths must be host-local absolute paths",
            ));
        }
        let lane = serde_json::to_string(&job.stream).map_err(io::Error::other)?;
        self.jobs.enqueue(QueueItem {
            key: job.job_id.to_string(),
            lane,
            payload: serde_json::to_vec(&job).map_err(io::Error::other)?,
        })
    }

    /// None means no admitted jobs remain. Errors retain the durable job and
    /// advance the lane cursor so the next call can service another stream.
    /// A completed job stays on disk; its receipt can be re-read from its
    /// JournalArchive after a caller loses the returned completion report.
    pub fn advance_one(&mut self) -> io::Result<Option<ArchiveAdvance>> {
        let lane = self
            .jobs
            .lanes()
            .find(|lane| self.last_lane.as_deref().is_none_or(|last| *lane > last))
            .or_else(|| self.jobs.lanes().next())
            .map(str::to_owned);
        let Some(lane) = lane else {
            return Ok(None);
        };
        self.last_lane = Some(lane.clone());
        let key = self
            .jobs
            .pending_keys(&lane, /*first_sequence*/ 0, NonZeroUsize::MIN)
            .first()
            .ok_or_else(|| io::Error::other("archive lane has no pending job"))?
            .to_string();
        let item = self.jobs.read(&key)?;
        let job: ArchiveJob = serde_json::from_slice(&item.payload).map_err(io::Error::other)?;
        if key != job.job_id.to_string()
            || lane != serde_json::to_string(&job.stream).map_err(io::Error::other)?
        {
            return Err(io::Error::other("archive job identity differs from queue"));
        }
        if !self.streams.contains_key(&lane) {
            self.streams.insert(
                lane.clone(),
                ActiveArchive {
                    source: job.source.clone(),
                    receipt_journal: job.receipt_journal.clone(),
                    archive: JournalArchive::open(&job.receipt_journal, job.stream)?,
                },
            );
        }
        let active = self
            .streams
            .get_mut(&lane)
            .ok_or_else(|| io::Error::other("active archive missing"))?;
        if active.source != job.source || active.receipt_journal != job.receipt_journal {
            return Err(io::Error::other("archive stream paths changed"));
        }
        let archive = &mut active.archive;
        let position = match job.target {
            ArchiveTarget::Snapshot(position) | ArchiveTarget::ProducerFinished(position) => {
                position
            }
        };
        if archive.require_archived(position).is_err() {
            archive.publish_next(&job.source, position, self.chunk_bytes, &mut self.shard)?;
        }
        let range = match archive.require_archived(position) {
            Ok(range) => range,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Ok(Some(ArchiveAdvance::Pending { job_id: job.job_id }));
            }
            Err(error) => return Err(error),
        };
        let receipt = match job.target {
            ArchiveTarget::Snapshot(_) => range.receipt.clone(),
            ArchiveTarget::ProducerFinished(_) => archive.seal(position, &mut self.shard)?,
        };
        self.jobs.complete(&key)?;
        if matches!(job.target, ArchiveTarget::ProducerFinished(_)) {
            self.streams.remove(&lane);
        }
        Ok(Some(ArchiveAdvance::Completed {
            job_id: job.job_id,
            receipt,
        }))
    }
}
