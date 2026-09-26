use std::io;
use std::path::Path;

use codex_infra_state::ArchiveReceipt;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;

use crate::ArchiveTarget;
use crate::TransportArchiveJobs;

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event {
    Prepared { jobs: Box<TransportArchiveJobs> },
    Completed { receipts: Box<[ArchiveReceipt; 7]> },
}

pub(super) struct ArchiveJournal {
    journal: Journal,
    pub(super) current: Option<TransportArchiveJobs>,
    completed: Option<[ArchiveReceipt; 7]>,
}

impl ArchiveJournal {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        let mut current = None;
        let mut completed = None;
        let journal = Journal::open(path, |record| {
            match serde_json::from_slice(&record.payload)? {
                Event::Prepared { jobs } => {
                    if current.is_some() && completed.is_none() {
                        return Err(io::Error::other("transport archive preparation overlaps"));
                    }
                    if let Some(previous) = &current {
                        check_binding(previous, &jobs)?;
                    }
                    current = Some(*jobs);
                    completed = None;
                }
                Event::Completed { receipts } => {
                    if current.is_none() || completed.is_some() {
                        return Err(io::Error::other(
                            "transport archive completion without pending jobs",
                        ));
                    }
                    completed = Some(*receipts);
                }
            }
            Ok(())
        })?;
        Ok(Self {
            journal,
            current,
            completed,
        })
    }

    pub(super) fn prepare(
        &mut self,
        jobs: TransportArchiveJobs,
    ) -> io::Result<TransportArchiveJobs> {
        if let Some(previous) = &self.current {
            check_binding(previous, &jobs)?;
            if self.completed.is_none()
                || previous
                    .jobs
                    .iter()
                    .zip(&jobs.jobs)
                    .all(|(a, b)| a.target == b.target)
            {
                return Ok(previous.clone());
            }
        }
        self.journal.append(&serde_json::to_vec(&Event::Prepared {
            jobs: Box::new(jobs.clone()),
        })?)?;
        self.current = Some(jobs.clone());
        self.completed = None;
        Ok(jobs)
    }

    pub(super) fn complete(&mut self, receipts: [ArchiveReceipt; 7]) -> io::Result<()> {
        if let Some(previous) = &self.completed {
            return if previous == &receipts {
                Ok(())
            } else {
                Err(io::Error::other("transport archive receipts changed"))
            };
        }
        if self.current.is_none() {
            return Err(io::Error::other("transport archive jobs missing"));
        }
        self.journal.append(&serde_json::to_vec(&Event::Completed {
            receipts: Box::new(receipts.clone()),
        })?)?;
        self.completed = Some(receipts);
        Ok(())
    }
}

fn check_binding(previous: &TransportArchiveJobs, next: &TransportArchiveJobs) -> io::Result<()> {
    for (a, b) in previous.jobs.iter().zip(&next.jobs) {
        if a.stream != b.stream || a.source != b.source || a.receipt_journal != b.receipt_journal {
            return Err(io::Error::other("transport archive stream binding changed"));
        }
        match (&a.target, &b.target) {
            (ArchiveTarget::Snapshot(previous), ArchiveTarget::Snapshot(next)) => {
                if next.byte_offset < previous.byte_offset
                    || next.next_sequence < previous.next_sequence
                {
                    return Err(io::Error::other("transport snapshot position regressed"));
                }
            }
            (ArchiveTarget::ProducerFinished(_), _) | (_, ArchiveTarget::ProducerFinished(_)) => {
                return Err(io::Error::other(
                    "transport snapshot pump cannot seal streams",
                ));
            }
        }
    }
    Ok(())
}
