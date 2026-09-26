use std::fs;
use std::fs::ReadDir;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_infra_state::QueueItem;
use codex_infra_state::SpoolQueue;

use crate::ArchiveJob;

#[derive(Clone, Copy)]
pub(super) enum Candidate {
    Dispatch(MessageId),
    Input(AgentId),
}

impl Candidate {
    fn lane(self) -> String {
        match self {
            Self::Dispatch(id) => format!("dispatch:{id}"),
            Self::Input(id) => format!("input:{id}"),
        }
    }
}

pub(super) struct Worker {
    sources: PathBuf,
    _binding: Journal,
    collectors: ReadDir,
    inputs: ReadDir,
    scan_inputs: bool,
    queue: SpoolQueue,
    last_lane: Option<String>,
}

impl Worker {
    pub(super) fn open(sources: &Path, directory: &Path) -> io::Result<Self> {
        let sources = sources.canonicalize()?;
        fs::create_dir_all(directory)?;
        let mut saved = false;
        let mut binding = Journal::open(&directory.join("binding.journal"), |record| {
            let previous: PathBuf = serde_json::from_slice(&record.payload)?;
            if saved || previous != sources {
                return Err(io::Error::other("shard archive source directory changed"));
            }
            saved = true;
            Ok(())
        })?;
        if !saved {
            binding.append(&serde_json::to_vec(&sources)?)?;
        }
        Ok(Self {
            collectors: fs::read_dir(sources.join("collectors"))?,
            inputs: fs::read_dir(sources.join("pane-input"))?,
            sources,
            _binding: binding,
            scan_inputs: false,
            queue: SpoolQueue::open(&directory.join("jobs.journal"))?,
            last_lane: None,
        })
    }

    /// One directory entry per step, alternating source classes. A new pass sees
    /// streams created since the previous pass without retaining file bodies.
    pub(super) fn discover(&mut self) -> io::Result<Option<Candidate>> {
        self.scan_inputs = !self.scan_inputs;
        let candidate = if self.scan_inputs {
            let Some(entry) = self.inputs.next() else {
                self.inputs = fs::read_dir(self.sources.join("pane-input"))?;
                return Ok(None);
            };
            let entry = entry?;
            if !entry.file_type()?.is_file()
                || entry.path().extension().is_none_or(|ext| ext != "journal")
            {
                return Ok(None);
            }
            Candidate::Input(
                entry
                    .path()
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| io::Error::other("input journal ID missing"))?
                    .parse()
                    .map_err(io::Error::other)?,
            )
        } else {
            let Some(entry) = self.collectors.next() else {
                self.collectors = fs::read_dir(self.sources.join("collectors"))?;
                return Ok(None);
            };
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                return Ok(None);
            }
            Candidate::Dispatch(
                entry
                    .file_name()
                    .to_str()
                    .ok_or_else(|| io::Error::other("collector ID missing"))?
                    .parse()
                    .map_err(io::Error::other)?,
            )
        };
        if !self
            .queue
            .pending_keys(&candidate.lane(), 0, NonZeroUsize::MIN)
            .is_empty()
        {
            return Ok(None);
        }
        Ok(Some(candidate))
    }

    pub(super) fn stage(&mut self, candidate: Candidate, jobs: Vec<ArchiveJob>) -> io::Result<()> {
        if jobs.is_empty()
            || jobs.len() > 3
            || jobs
                .iter()
                .any(|job| !job.source.starts_with(&self.sources))
        {
            return Err(io::Error::other(
                "shard archive jobs differ from discovery binding",
            ));
        }
        let lane = candidate.lane();
        // Job IDs are excluded: the same producer positions retain the original
        // prepared IDs, even after completion or a lost admission response.
        let positions: Vec<_> = jobs.iter().map(|job| (&job.stream, &job.target)).collect();
        let key = format!("{lane}:{}", serde_json::to_string(&positions)?);
        match self.queue.read(&key) {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        self.queue.enqueue(QueueItem {
            key,
            lane,
            payload: serde_json::to_vec(&jobs)?,
        })?;
        Ok(())
    }

    pub(super) fn pending(&mut self) -> io::Result<Option<(String, Vec<ArchiveJob>)>> {
        let lane = self
            .queue
            .lanes()
            .find(|lane| self.last_lane.as_deref().is_none_or(|last| *lane > last))
            .or_else(|| self.queue.lanes().next())
            .map(str::to_owned);
        let Some(lane) = lane else {
            return Ok(None);
        };
        self.last_lane = Some(lane.clone());
        let keys = self.queue.pending_keys(&lane, 0, NonZeroUsize::MIN);
        let key = keys
            .first()
            .ok_or_else(|| io::Error::other("shard archive lane empty"))?;
        let item = self.queue.read(key)?;
        Ok(Some((item.key, serde_json::from_slice(&item.payload)?)))
    }

    pub(super) fn complete(&mut self, key: &str) -> io::Result<()> {
        self.queue.complete(key)
    }
}
