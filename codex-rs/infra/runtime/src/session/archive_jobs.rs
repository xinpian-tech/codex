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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportArchiveJobIds {
    pub directory: MessageId,
    pub readiness: MessageId,
    pub network: MessageId,
    pub network_cursor: MessageId,
    pub inbox: MessageId,
    pub observations: MessageId,
    pub binding: MessageId,
}

/// Persist the exact prepared jobs before submission. Order is directory,
/// readiness, network events/cursor, inbox, observations and archive binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportArchiveJobs {
    pub jobs: [ArchiveJob; 7],
}

impl TransportArchiveJobs {
    pub async fn submit(&self, controller: &ArchiveController) -> io::Result<()> {
        for job in &self.jobs {
            controller.enqueue(job.clone()).await?;
        }
        Ok(())
    }

    pub async fn completion(
        &self,
        controller: &ArchiveController,
    ) -> io::Result<Option<[ArchiveReceipt; 7]>> {
        let mut receipts = Vec::with_capacity(7);
        for job in &self.jobs {
            let Some(receipt) = controller.completion(job.job_id).await? else {
                return Ok(None);
            };
            receipts.push(receipt);
        }
        receipts
            .try_into()
            .map(Some)
            .map_err(|_| io::Error::other("transport archive receipt set is incomplete"))
    }
}

impl TransportSession {
    /// Samples positions from the owned durable writers, including their actual
    /// spool binding. These are snapshots: directory updates, tail extraction and
    /// later shutdown observations can still append after sampling.
    pub fn prepare_archive_snapshot(
        &self,
        receipts: &Path,
        ids: TransportArchiveJobIds,
    ) -> io::Result<TransportArchiveJobs> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "transport receipt directory must be absolute",
            ));
        }
        let streams = [
            ("directory", ids.directory, self.directory.position()),
            ("launches", ids.readiness, self.readiness.position()),
            ("network", ids.network, self.ingress.event_position()),
            (
                "network-cursor",
                ids.network_cursor,
                self.ingress.checkpoint_position(),
            ),
            ("network-inbox", ids.inbox, self.inbox.position()),
            ("transport", ids.observations, self.observations.position()),
            (
                "archive-binding",
                ids.binding,
                self.archive_binding.journal.position(),
            ),
        ];
        let machine_run_id = self.archive_binding.machine_run_id;
        Ok(TransportArchiveJobs {
            jobs: streams.map(|(name, job_id, position)| ArchiveJob {
                job_id,
                stream: ArchiveStream {
                    root_session_id: self.config.root_session_id,
                    machine_id: self.config.machine_id.clone(),
                    producer: ArchiveProducer::Machine { machine_run_id },
                    name: format!("transport-{name}"),
                },
                source: self.config.directory.join(format!("{name}.journal")),
                receipt_journal: receipts
                    .join(format!("{machine_run_id}-transport-{name}.journal")),
                target: ArchiveTarget::Snapshot(position),
            }),
        })
    }
}
