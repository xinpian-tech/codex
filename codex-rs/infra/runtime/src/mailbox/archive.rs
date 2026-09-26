use std::io;
use std::io::Write;
use std::path::Path;

use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_state::ArchiveReceipt;
use codex_infra_state::ArchiveStream;
use serde::Deserialize;
use serde::Serialize;

use crate::ArchiveController;
use crate::ArchiveJob;
use crate::ArchiveTarget;

use super::HostMailbox;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxArchiveJobIds {
    pub stdin: MessageId,
    pub stdout: MessageId,
    pub inbox: MessageId,
    pub outbox: MessageId,
    pub publications: MessageId,
}

/// Persist before admission and replay these exact jobs after interruption.
/// The array contains stdin, stdout, inbox, outbox and publication-intent jobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxArchiveJobs {
    pub jobs: [ArchiveJob; 5],
}

impl MailboxArchiveJobs {
    pub async fn submit(&self, controller: &ArchiveController) -> io::Result<()> {
        for job in &self.jobs {
            controller.enqueue(job.clone()).await?;
        }
        Ok(())
    }

    /// Returns all five exact job receipts in job order, or None while any job
    /// remains pending. This does not infer receiver delivery from local stdout.
    pub async fn completion(
        &self,
        controller: &ArchiveController,
    ) -> io::Result<Option<[ArchiveReceipt; 5]>> {
        let mut receipts = Vec::with_capacity(5);
        for job in &self.jobs {
            let Some(receipt) = controller.completion(job.job_id).await? else {
                return Ok(None);
            };
            receipts.push(receipt);
        }
        receipts
            .try_into()
            .map(Some)
            .map_err(|_| io::Error::other("mailbox archive receipt set is incomplete"))
    }
}

impl<W: Write> HostMailbox<W> {
    /// Consumes the mailbox after input and publication workers have released
    /// it. The final flush is included before closing every owned journal.
    /// Persist the returned jobs before submitting them to the archive actor.
    pub fn finish_archive(
        mut self,
        machine_id: MachineId,
        launch_id: MessageId,
        receipts: &Path,
        ids: MailboxArchiveJobIds,
    ) -> io::Result<MailboxArchiveJobs> {
        let mut jobs = self.prepare_archive_snapshot(machine_id, launch_id, receipts, ids)?;
        for job in &mut jobs.jobs {
            let position = match job.target {
                ArchiveTarget::Snapshot(position) | ArchiveTarget::ProducerFinished(position) => {
                    position
                }
            };
            job.target = ArchiveTarget::ProducerFinished(position);
        }
        drop(self);
        Ok(jobs)
    }

    /// Uses this mailbox's actual spool directory and producer positions.
    /// Machine/launch come from the host's immutable runtime binding. Receipt
    /// directory already exists locally. These remain snapshots because further
    /// receipts, final-message bytes, or input can arrive after this sample.
    pub fn prepare_archive_snapshot(
        &mut self,
        machine_id: MachineId,
        launch_id: MessageId,
        receipts: &Path,
        ids: MailboxArchiveJobIds,
    ) -> io::Result<MailboxArchiveJobs> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "mailbox archive receipt directory must be absolute",
            ));
        }
        let positions = self.flush_positions()?;
        let entries = [
            ("stdin", ids.stdin, positions.stdin),
            ("stdout", ids.stdout, positions.stdout),
            ("inbox", ids.inbox, positions.inbox),
            ("outbox", ids.outbox, positions.outbox),
            ("publications", ids.publications, positions.publications),
        ];
        Ok(MailboxArchiveJobs {
            jobs: entries.map(|(name, job_id, position)| ArchiveJob {
                job_id,
                stream: ArchiveStream {
                    root_session_id: self.root_session_id,
                    machine_id: machine_id.clone(),
                    agent_id: self.agent_id,
                    launch_id,
                    name: format!("mailbox-{name}"),
                },
                source: self.directory.join(format!("{name}.journal")),
                receipt_journal: receipts.join(format!("{launch_id}-mailbox-{name}.journal")),
                target: ArchiveTarget::Snapshot(position),
            }),
        })
    }
}
