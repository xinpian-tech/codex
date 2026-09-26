use std::fs;
use std::fs::ReadDir;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Journal;
use codex_infra_state::QueueItem;
use codex_infra_state::SpoolQueue;
use serde::Deserialize;
use serde::Serialize;

use super::CollectorArchiveJobIds;
use super::CollectorArchiveJobs;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Binding {
    root_session_id: RootSessionId,
    machine_id: MachineId,
    collectors: PathBuf,
}

/// Local durable preparation queue for completed collector attachments. Bodies
/// stay on disk; discovery reads one directory entry per advance and pending
/// attachments rotate independently while the machine writer archives bytes.
pub struct CollectorArchiveWorker {
    binding: Binding,
    _binding_journal: Journal,
    receipts: PathBuf,
    discovery: ReadDir,
    queue: SpoolQueue,
    last_lane: Option<String>,
}

impl CollectorArchiveWorker {
    pub(crate) fn open(
        session_directory: &Path,
        root_session_id: RootSessionId,
        machine_id: MachineId,
    ) -> io::Result<Self> {
        let binding = Binding {
            root_session_id,
            machine_id,
            collectors: session_directory.join("collectors").canonicalize()?,
        };
        let directory = session_directory.join("collector-archive");
        let receipts = directory.join("receipts");
        fs::create_dir_all(&receipts)?;
        let mut opened = false;
        let mut binding_journal = Journal::open(&directory.join("binding.journal"), |record| {
            let previous: Binding = serde_json::from_slice(&record.payload)?;
            if opened || previous != binding {
                return Err(io::Error::other("collector archive binding changed"));
            }
            opened = true;
            Ok(())
        })?;
        if !opened {
            binding_journal.append(&serde_json::to_vec(&binding)?)?;
        }
        Ok(Self {
            discovery: fs::read_dir(&binding.collectors)?,
            binding,
            _binding_journal: binding_journal,
            receipts: receipts.canonicalize()?,
            queue: SpoolQueue::open(&directory.join("jobs.journal"))?,
            last_lane: None,
        })
    }

    /// A complete scan restarts so attachments still running on an earlier pass
    /// are eventually picked up. Existing jobs, including completed ones, retain
    /// their original IDs and producer positions instead of being sampled again.
    pub(super) fn discover_one(&mut self) -> io::Result<()> {
        let Some(entry) = self.discovery.next() else {
            self.discovery = fs::read_dir(&self.binding.collectors)?;
            return Ok(());
        };
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            return Ok(());
        }
        let attachment_id: MessageId = entry
            .file_name()
            .to_str()
            .ok_or_else(|| io::Error::other("collector ID is not UTF-8"))?
            .parse()
            .map_err(io::Error::other)?;
        let key = attachment_id.to_string();
        match self.queue.read(&key) {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let Some(jobs) = CollectorArchiveJobs::prepare(
            &entry.path(),
            attachment_id,
            self.binding.root_session_id,
            self.binding.machine_id.clone(),
            &self.receipts,
            CollectorArchiveJobIds {
                stdout: MessageId::new(),
                stderr: MessageId::new(),
                commands: MessageId::new(),
                lifecycle: MessageId::new(),
                completion: MessageId::new(),
            },
        )?
        else {
            return Ok(());
        };
        self.queue.enqueue(QueueItem {
            lane: key.clone(),
            key,
            payload: serde_json::to_vec(&jobs)?,
        })?;
        Ok(())
    }

    pub(super) fn next_pending(&mut self) -> io::Result<Option<(String, CollectorArchiveJobs)>> {
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
            .ok_or_else(|| io::Error::other("collector archive lane is empty"))?;
        let item = self.queue.read(key)?;
        let jobs = serde_json::from_slice(&item.payload)?;
        Ok(Some((item.key, jobs)))
    }

    /// Called only after all five jobs report their durable remote receipts.
    pub(super) fn complete(&mut self, key: &str) -> io::Result<()> {
        self.queue.complete(key)
    }
}
