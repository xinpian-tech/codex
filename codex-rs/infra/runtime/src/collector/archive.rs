use std::io;
use std::path::Path;

use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveReceipt;
use codex_infra_state::ArchiveStream;
use serde::Deserialize;
use serde::Serialize;

use super::CollectorFinished;
use super::progress;
use crate::ArchiveController;
use crate::ArchiveJob;
use crate::ArchiveTarget;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorArchiveJobIds {
    pub stdout: MessageId,
    pub stderr: MessageId,
    pub commands: MessageId,
    pub lifecycle: MessageId,
    pub completion: MessageId,
}

/// Machine-owned control streams in stdout, stderr, commands, lifecycle order,
/// plus the completion marker for finished producers. Persist before submission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorArchiveJobs {
    pub jobs: Vec<ArchiveJob>,
}

impl CollectorArchiveJobs {
    /// Reads the collector's durable completion, including its marker position.
    /// An attachment without that record remains unfinished. Root/machine come
    /// from the owning transport runtime, not from an arbitrary Agent pane.
    pub fn prepare(
        directory: &Path,
        attachment_id: MessageId,
        root_session_id: RootSessionId,
        machine_id: MachineId,
        receipts: &Path,
        ids: CollectorArchiveJobIds,
    ) -> io::Result<Option<Self>> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "collector receipt directory must be absolute",
            ));
        }
        let directory = directory.canonicalize()?;
        let Some((finished, marker_position)) =
            CollectorFinished::read_with_position(&directory, attachment_id)?
        else {
            return Ok(None);
        };
        let streams = [
            ("stdout", ids.stdout, finished.stdout_position),
            ("stderr", ids.stderr, finished.stderr_position),
            ("stdin", ids.commands, finished.commands_position),
            ("lifecycle", ids.lifecycle, finished.lifecycle_position),
            ("completion", ids.completion, marker_position),
        ];
        Ok(Some(Self {
            jobs: streams
                .map(|(name, job_id, position)| ArchiveJob {
                    job_id,
                    stream: ArchiveStream {
                        root_session_id,
                        machine_id: machine_id.clone(),
                        producer: ArchiveProducer::Machine {
                            machine_run_id: attachment_id,
                        },
                        name: format!("collector-{name}"),
                    },
                    source: directory.join(format!("{name}.journal")),
                    receipt_journal: receipts
                        .join(format!("{attachment_id}-collector-{name}.journal")),
                    target: ArchiveTarget::ProducerFinished(position),
                })
                .into(),
        }))
    }

    /// Samples only producer-published durable prefixes. No stream is sealed,
    /// even if the collector exits while these independent positions are read.
    pub fn prepare_snapshot(
        directory: &Path,
        attachment_id: MessageId,
        root_session_id: RootSessionId,
        machine_id: MachineId,
        receipts: &Path,
        ids: CollectorArchiveJobIds,
    ) -> io::Result<Option<Self>> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "collector receipt directory must be absolute",
            ));
        }
        let directory = directory.canonicalize()?;
        let mut jobs = Vec::with_capacity(4);
        for (name, job_id) in [
            ("stdout", ids.stdout),
            ("stderr", ids.stderr),
            ("stdin", ids.commands),
            ("lifecycle", ids.lifecycle),
        ] {
            let Some(position) = progress::read(&directory, name, attachment_id)? else {
                return Ok(None);
            };
            jobs.push(ArchiveJob {
                job_id,
                stream: ArchiveStream {
                    root_session_id,
                    machine_id: machine_id.clone(),
                    producer: ArchiveProducer::Machine {
                        machine_run_id: attachment_id,
                    },
                    name: format!("collector-{name}"),
                },
                source: directory.join(format!("{name}.journal")),
                receipt_journal: receipts.join(format!("{attachment_id}-collector-{name}.journal")),
                target: ArchiveTarget::Snapshot(position),
            });
        }
        Ok(Some(Self { jobs }))
    }

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
