use std::io;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_runtime::ArchiveJob;
use codex_infra_runtime::ArchiveTarget;
use codex_infra_runtime::LaunchIntent;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;
use codex_infra_state::Journal;

use super::PreparedAgentHost;
use super::StartedAgentHost;

impl PreparedAgentHost {
    /// Preparation is immutable after open. Archive it even when later account
    /// or inference startup fails; keep its exact job for finalization replay.
    pub fn preparation_archive_job(
        &self,
        receipts: &Path,
        job_id: MessageId,
    ) -> io::Result<ArchiveJob> {
        build(
            &self.launch,
            &self.directory,
            self.services.launch_binding.as_deref(),
            receipts,
            job_id,
        )
    }
}

impl StartedAgentHost {
    /// The same immutable preparation prefix remains available after startup.
    /// Enqueueing this job does not finish the host's mutable audit streams.
    pub fn preparation_archive_job(
        &self,
        receipts: &Path,
        job_id: MessageId,
    ) -> io::Result<ArchiveJob> {
        build(
            &self.launch,
            &self.directory,
            self.host._launch_binding.as_deref(),
            receipts,
            job_id,
        )
    }
}

fn build(
    launch: &LaunchIntent,
    directory: &Path,
    journal: Option<&Journal>,
    receipts: &Path,
    job_id: MessageId,
) -> io::Result<ArchiveJob> {
    if !receipts.is_absolute() {
        return Err(io::Error::other(
            "preparation receipt directory must be absolute",
        ));
    }
    let journal = journal.ok_or_else(|| io::Error::other("preparation binding is absent"))?;
    Ok(ArchiveJob {
        job_id,
        stream: ArchiveStream {
            root_session_id: launch.workspace.root_session_id,
            machine_id: launch.machine_id.clone(),
            producer: ArchiveProducer::Agent {
                agent_id: launch.workspace.agent_id,
                launch_id: launch.launch_id,
            },
            name: "preparation".to_owned(),
        },
        source: directory.join("preparation.journal"),
        receipt_journal: receipts.join(format!("{}-preparation.journal", launch.launch_id)),
        target: ArchiveTarget::ProducerFinished(journal.position()),
    })
}
