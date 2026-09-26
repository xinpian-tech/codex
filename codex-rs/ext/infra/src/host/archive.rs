use std::io;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_runtime::ArchiveController;
use codex_infra_runtime::ArchiveJob;
use codex_infra_runtime::ArchiveTarget;
use codex_infra_state::ArchiveReceipt;
use codex_infra_state::ArchiveStream;
use serde::Deserialize;
use serde::Serialize;

use super::ManagedHost;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostArchiveJobIds {
    pub processes: MessageId,
    pub tools: MessageId,
    pub thread_store: MessageId,
}

pub enum HostArchivePhase {
    Snapshot,
    ProducerFinished,
}

/// Persist the prepared jobs with finalization state before submitting them.
/// Replay submits these exact jobs, not a fresh sample with the same IDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostArchiveJobs {
    pub processes: ArchiveJob,
    pub tools: ArchiveJob,
    pub thread_store: ArchiveJob,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostArchiveReceipts {
    pub processes: ArchiveReceipt,
    pub tools: ArchiveReceipt,
    pub thread_store: ArchiveReceipt,
}

impl HostArchiveJobs {
    /// None means at least one exact job is still pending. Completed receipts
    /// come from the machine's durable result journal, not actor liveness.
    pub async fn completion(
        &self,
        controller: &ArchiveController,
    ) -> io::Result<Option<HostArchiveReceipts>> {
        let Some(processes) = controller.completion(self.processes.job_id).await? else {
            return Ok(None);
        };
        let Some(tools) = controller.completion(self.tools.job_id).await? else {
            return Ok(None);
        };
        let Some(thread_store) = controller.completion(self.thread_store.job_id).await? else {
            return Ok(None);
        };
        Ok(Some(HostArchiveReceipts {
            processes,
            tools,
            thread_store,
        }))
    }

    pub async fn submit(&self, controller: &ArchiveController) -> io::Result<()> {
        for job in [&self.processes, &self.tools, &self.thread_store] {
            controller.enqueue(job.clone()).await?;
        }
        Ok(())
    }
}

impl ManagedHost {
    /// Captures the actual open audit paths and settled producer positions.
    /// The receipt directory must already exist on this machine. Snapshot
    /// requires quiescent dispatch; ProducerFinished additionally requires that
    /// the caller has stopped these producers, including thread-store shutdown.
    pub fn prepare_archive_jobs(
        &self,
        receipts: &Path,
        ids: HostArchiveJobIds,
        phase: HostArchivePhase,
    ) -> io::Result<HostArchiveJobs> {
        prepare_jobs(
            &self.processes,
            &self.tools,
            &self.store_audit,
            receipts,
            ids,
            phase,
        )
    }
}

pub(super) fn prepare_jobs(
    processes: &crate::ProcessAudit,
    tools: &crate::ToolAudit,
    store: &crate::StoreAudit,
    receipts: &Path,
    ids: HostArchiveJobIds,
    phase: HostArchivePhase,
) -> io::Result<HostArchiveJobs> {
    if !receipts.is_absolute() {
        return Err(io::Error::other(
            "archive receipt directory must be absolute",
        ));
    }
    let positions = super::HostAuditPositions {
        processes: processes.settled_position()?,
        tools: tools.settled_position()?,
        thread_store: store.settled_position()?,
    };
    let identity = &processes.identity;
    let launch_id = processes.launch_id;
    let build = |name: &str, source, job_id, position| ArchiveJob {
        job_id,
        stream: ArchiveStream {
            root_session_id: identity.root_session_id,
            machine_id: identity.machine_id.clone(),
            agent_id: identity.agent_id,
            launch_id,
            name: name.to_owned(),
        },
        source,
        receipt_journal: receipts.join(format!("{launch_id}-{name}.journal")),
        target: match phase {
            HostArchivePhase::Snapshot => ArchiveTarget::Snapshot(position),
            HostArchivePhase::ProducerFinished => ArchiveTarget::ProducerFinished(position),
        },
    };
    Ok(HostArchiveJobs {
        processes: build(
            "processes",
            processes.path.clone(),
            ids.processes,
            positions.processes,
        ),
        tools: build("tools", tools.path.clone(), ids.tools, positions.tools),
        thread_store: build(
            "thread-store",
            store.path.clone(),
            ids.thread_store,
            positions.thread_store,
        ),
    })
}
