use std::io;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_provider::ProviderAttemptFinished;
use codex_infra_provider::ProviderAttemptIdentity;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveReceipt;
use codex_infra_state::ArchiveStream;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;

use crate::ArchiveController;
use crate::ArchiveJob;
use crate::ArchiveTarget;

mod actor;
pub use actor::ProviderArchiveActor;
pub use actor::ProviderArchiveConfig;

/// One completed attempt's five audit streams and producer completion marker.
/// Job identities are persisted in the source spool before queue admission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderArchiveJobs {
    pub attempt_id: MessageId,
    pub jobs: [ArchiveJob; 6],
}

impl ProviderArchiveJobs {
    /// Blocking preparation using producer-confirmed positions. A missing
    /// completion remains pending; recovery is owned by the provider producer.
    pub fn prepare(directory: &Path, receipts: &Path) -> io::Result<Option<Self>> {
        let directory = directory.canonicalize()?;
        let Some((finished, marker)) = ProviderAttemptFinished::read_with_position(&directory)?
        else {
            return Ok(None);
        };
        let identity = ProviderAttemptIdentity::read(&directory)?
            .ok_or_else(|| io::Error::other("provider completion without attempt identity"))?;
        if identity.attempt_id != finished.attempt_id {
            return Err(io::Error::other("provider completion identity mismatch"));
        }
        std::fs::create_dir_all(receipts)?;
        let receipts = receipts.canonicalize()?;
        let mut saved = None::<Self>;
        let mut journal = Journal::open(&directory.join("archive.journal"), |record| {
            if saved.is_some() {
                return Err(io::Error::other(
                    "provider archive has multiple preparations",
                ));
            }
            saved = Some(serde_json::from_slice(&record.payload)?);
            Ok(())
        })?;
        let names = [
            "client-request",
            "provider-request",
            "provider-response",
            "client-response",
            "lifecycle",
            "completion",
        ];
        let mut jobs = Vec::with_capacity(names.len());
        for name in names {
            let position = if name == "completion" {
                marker
            } else {
                *finished.positions.get(name).ok_or_else(|| {
                    io::Error::other(format!("provider completion missing {name}"))
                })?
            };
            jobs.push(ArchiveJob {
                job_id: MessageId::new(),
                stream: ArchiveStream {
                    root_session_id: identity.root_session_id,
                    machine_id: identity.machine_id.clone(),
                    producer: ArchiveProducer::Agent {
                        agent_id: identity.agent_id,
                        launch_id: identity.launch_id,
                    },
                    name: format!("provider-{}-{name}", identity.attempt_id),
                },
                source: directory.join(format!("{name}.journal")),
                receipt_journal: receipts.join(format!("{}-{name}.journal", identity.attempt_id)),
                target: ArchiveTarget::ProducerFinished(position),
            });
        }
        if let Some(saved) = saved {
            if saved.attempt_id != identity.attempt_id {
                return Err(io::Error::other("saved provider archive identity changed"));
            }
            for (saved, current) in saved.jobs.iter().zip(&jobs) {
                if saved.stream != current.stream
                    || saved.source != current.source
                    || saved.receipt_journal != current.receipt_journal
                    || saved.target != current.target
                {
                    return Err(io::Error::other("saved provider archive binding changed"));
                }
            }
            return Ok(Some(saved));
        }
        let prepared = Self {
            attempt_id: identity.attempt_id,
            jobs: jobs
                .try_into()
                .map_err(|_| io::Error::other("provider archive stream count"))?,
        };
        journal.append(&serde_json::to_vec(&prepared)?)?;
        Ok(Some(prepared))
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
