use std::io;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveReceipt;
use codex_infra_state::ArchiveStream;
use serde::Deserialize;
use serde::Serialize;

use super::TransportSession;
use crate::ArchiveController;
use crate::ArchiveJob;
use crate::ArchiveTarget;
use crate::dispatch::DispatchCompletion;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchArchiveJobIds {
    pub queue: MessageId,
    pub cursor: MessageId,
    pub completion: MessageId,
}

/// Queue/cursor snapshots, or both ended streams plus their retirement marker.
/// Save these exact jobs before submitting them to the machine archive actor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchArchiveJobs {
    pub jobs: Vec<ArchiveJob>,
}

impl DispatchArchiveJobs {
    pub async fn submit(&self, controller: &ArchiveController) -> io::Result<()> {
        for job in &self.jobs {
            controller.enqueue(job.clone()).await?;
        }
        Ok(())
    }

    pub async fn completion(
        &self,
        controller: &ArchiveController,
    ) -> io::Result<Option<Vec<ArchiveReceipt>>> {
        let mut receipts = Vec::with_capacity(self.jobs.len());
        for job in &self.jobs {
            let Some(receipt) = controller.completion(job.job_id).await? else {
                return Ok(None);
            };
            receipts.push(receipt);
        }
        Ok(Some(receipts))
    }
}

impl TransportSession {
    /// Active attachments use owned writer acknowledgments. Retired attachments
    /// use their immutable marker, which is written before removing the writer.
    /// A missing historical marker remains unresolved rather than guessing EOF.
    pub fn prepare_dispatch_archive(
        &self,
        attachment_id: MessageId,
        receipts: &Path,
        ids: DispatchArchiveJobIds,
    ) -> io::Result<Option<DispatchArchiveJobs>> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "dispatch receipt directory must be absolute",
            ));
        }
        let directory = self
            .config
            .directory
            .join("collectors")
            .join(attachment_id.to_string());
        let targets = if let Some(attachment) = self.attachments.get(&attachment_id) {
            let positions = attachment.dispatch.positions();
            vec![
                (
                    "dispatch",
                    ids.queue,
                    ArchiveTarget::Snapshot(positions.queue),
                ),
                (
                    "dispatch-cursor",
                    ids.cursor,
                    ArchiveTarget::Snapshot(positions.cursor),
                ),
            ]
        } else {
            let Some((positions, marker)) = DispatchCompletion::read(
                &directory.join("dispatch-completion.journal"),
                attachment_id,
            )?
            else {
                return Ok(None);
            };
            vec![
                (
                    "dispatch",
                    ids.queue,
                    ArchiveTarget::ProducerFinished(positions.queue),
                ),
                (
                    "dispatch-cursor",
                    ids.cursor,
                    ArchiveTarget::ProducerFinished(positions.cursor),
                ),
                (
                    "dispatch-completion",
                    ids.completion,
                    ArchiveTarget::ProducerFinished(marker),
                ),
            ]
        };
        let jobs = targets
            .into_iter()
            .map(|(name, job_id, target)| ArchiveJob {
                job_id,
                stream: ArchiveStream {
                    root_session_id: self.config.root_session_id,
                    machine_id: self.config.machine_id.clone(),
                    producer: ArchiveProducer::Machine {
                        machine_run_id: attachment_id,
                    },
                    name: format!("collector-{name}"),
                },
                source: directory.join(format!("{name}.journal")),
                receipt_journal: receipts.join(format!("{attachment_id}-collector-{name}.journal")),
                target,
            })
            .collect();
        Ok(Some(DispatchArchiveJobs { jobs }))
    }
}
