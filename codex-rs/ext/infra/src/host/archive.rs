use std::io;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_runtime::ArchiveController;
use codex_infra_runtime::ArchiveJob;
use codex_infra_runtime::ArchiveTarget;
use codex_infra_state::ArchiveProducer;
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
    #[serde(default)]
    pub account: Option<MessageId>,
    #[serde(default)]
    pub rpc: Option<MessageId>,
}

#[derive(Clone, Copy)]
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
    #[serde(default)]
    pub account: Option<ArchiveJob>,
    #[serde(default)]
    pub rpc: Option<ArchiveJob>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostArchiveReceipts {
    pub processes: ArchiveReceipt,
    pub tools: ArchiveReceipt,
    pub thread_store: ArchiveReceipt,
    #[serde(default)]
    pub account: Option<ArchiveReceipt>,
    #[serde(default)]
    pub rpc: Option<ArchiveReceipt>,
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
        let account = match &self.account {
            Some(job) => match controller.completion(job.job_id).await? {
                Some(receipt) => Some(receipt),
                None => return Ok(None),
            },
            None => None,
        };
        let rpc = match &self.rpc {
            Some(job) => match controller.completion(job.job_id).await? {
                Some(receipt) => Some(receipt),
                None => return Ok(None),
            },
            None => None,
        };
        Ok(Some(HostArchiveReceipts {
            processes,
            tools,
            thread_store,
            account,
            rpc,
        }))
    }

    pub async fn submit(&self, controller: &ArchiveController) -> io::Result<()> {
        for job in [&self.processes, &self.tools, &self.thread_store]
            .into_iter()
            .chain(self.account.iter())
            .chain(self.rpc.iter())
        {
            controller.enqueue(job.clone()).await?;
        }
        Ok(())
    }

    pub(super) fn attach_rpc(
        &mut self,
        rpc: &crate::AgentRpc,
        receipts: &Path,
        job_id: Option<MessageId>,
        phase: HostArchivePhase,
    ) -> io::Result<()> {
        let job_id =
            job_id.ok_or_else(|| io::Error::other("host RPC archive job ID is required"))?;
        let (source, position) = rpc.snapshot()?;
        let mut stream = self.processes.stream.clone();
        stream.name = "rpc".to_owned();
        let ArchiveProducer::Agent { launch_id, .. } = stream.producer else {
            return Err(io::Error::other("host archive producer is not an Agent"));
        };
        self.rpc = Some(ArchiveJob {
            job_id,
            stream,
            source,
            receipt_journal: receipts.join(format!("{launch_id}-rpc.journal")),
            target: match phase {
                HostArchivePhase::Snapshot => ArchiveTarget::Snapshot(position),
                HostArchivePhase::ProducerFinished => ArchiveTarget::ProducerFinished(position),
            },
        });
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
        let rpc_id = ids.rpc;
        let mut jobs = prepare_jobs(
            &self.processes,
            &self.tools,
            &self.store_audit,
            self.account_observation.as_ref(),
            receipts,
            ids,
            phase,
        )?;
        jobs.attach_rpc(&self.rpc, receipts, rpc_id, phase)?;
        Ok(jobs)
    }
}

pub(super) fn prepare_jobs(
    processes: &crate::ProcessAudit,
    tools: &crate::ToolAudit,
    store: &crate::StoreAudit,
    account: Option<&codex_infra_account::AccountObservation>,
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
            producer: ArchiveProducer::Agent {
                agent_id: identity.agent_id,
                launch_id,
            },
            name: name.to_owned(),
        },
        source,
        receipt_journal: receipts.join(format!("{launch_id}-{name}.journal")),
        target: match phase {
            HostArchivePhase::Snapshot => ArchiveTarget::Snapshot(position),
            HostArchivePhase::ProducerFinished => ArchiveTarget::ProducerFinished(position),
        },
    };
    let account = match (account, ids.account) {
        (Some(observation), Some(job_id)) => {
            let (source, position) = observation.settled_snapshot()?;
            Some(build("account-observations", source, job_id, position))
        }
        (None, None) => None,
        (Some(_), None) | (None, Some(_)) => {
            return Err(io::Error::other(
                "account archive job id differs from host account",
            ));
        }
    };
    Ok(HostArchiveJobs {
        account,
        rpc: None,
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
