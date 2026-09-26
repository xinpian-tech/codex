use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_protocol::MessageId;
use codex_infra_state::ArchiveReceipt;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
struct Completion {
    job_id: MessageId,
    receipt: ArchiveReceipt,
}

pub(super) struct ArchiveReceipts {
    journal: Journal,
    path: PathBuf,
    positions: BTreeMap<MessageId, JournalPosition>,
}

impl ArchiveReceipts {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        let mut positions = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            let completion: Completion =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            if positions
                .insert(completion.job_id, record.position)
                .is_some()
            {
                return Err(io::Error::other("archive job completion recorded twice"));
            }
            Ok(())
        })?;
        Ok(Self {
            journal,
            path: path.canonicalize()?,
            positions,
        })
    }

    pub(super) fn get(&self, job_id: MessageId) -> io::Result<Option<ArchiveReceipt>> {
        let Some(position) = self.positions.get(&job_id) else {
            return Ok(None);
        };
        let record = JournalReader::open(&self.path, *position)?
            .next_record()?
            .ok_or_else(|| io::Error::other("archive completion payload missing"))?;
        let completion: Completion =
            serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
        if completion.job_id != job_id {
            return Err(io::Error::other(
                "archive completion offset differs from job",
            ));
        }
        Ok(Some(completion.receipt))
    }

    pub(super) fn record(&mut self, job_id: MessageId, receipt: ArchiveReceipt) -> io::Result<()> {
        if let Some(previous) = self.get(job_id)? {
            return if previous == receipt {
                Ok(())
            } else {
                Err(io::Error::other("archive job receipt changed"))
            };
        }
        let position = self.journal.position();
        self.journal.append(
            &serde_json::to_vec(&Completion { job_id, receipt }).map_err(io::Error::other)?,
        )?;
        self.positions.insert(job_id, position);
        Ok(())
    }
}
