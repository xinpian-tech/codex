use std::io;
use std::path::PathBuf;

use codex_infra_protocol::MessageId;
use codex_infra_runtime::ArchiveController;
use codex_infra_runtime::ArchiveJob;
use codex_infra_runtime::ArchiveTarget;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveReceipt;
use codex_infra_state::ArchiveStream;
use serde::Deserialize;
use serde::Serialize;

use super::StartedAgentHost;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDriverArchiveJobIds {
    pub thread: MessageId,
    pub inputs: MessageId,
}

/// Final prefixes from closed thread and input drivers. Persist these exact
/// jobs before enqueueing, and replay them when resuming finalization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDriverArchiveJobs {
    pub thread: ArchiveJob,
    pub inputs: ArchiveJob,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDriverArchiveReceipts {
    pub thread: ArchiveReceipt,
    pub inputs: ArchiveReceipt,
}

impl StartedAgentHost {
    /// Stop the terminal dispatch loop before calling. This owns closure of
    /// both drivers through completion even when the caller stops waiting.
    /// Call before host shutdown: admitted driver operations still need RPC.
    /// Closing these drivers neither stops inference nor finishes the Agent.
    pub async fn close_input_drivers(
        &self,
        receipts: PathBuf,
        ids: AgentDriverArchiveJobIds,
    ) -> io::Result<AgentDriverArchiveJobs> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "driver receipt directory must be absolute",
            ));
        }
        let inputs = self.inputs.clone();
        let thread = self.thread.clone();
        let launch = self.launch.clone();
        tokio::spawn(async move {
            // Attempt both closes even if the first journal reports an error.
            let input_end = inputs.close().await;
            let thread_end = thread.close().await;
            let input_end = input_end?;
            let thread_end = thread_end?;
            let build = |name: &str, job_id, (source, position)| ArchiveJob {
                job_id,
                stream: ArchiveStream {
                    root_session_id: launch.workspace.root_session_id,
                    machine_id: launch.machine_id.clone(),
                    producer: ArchiveProducer::Agent {
                        agent_id: launch.workspace.agent_id,
                        launch_id: launch.launch_id,
                    },
                    name: name.to_owned(),
                },
                source,
                receipt_journal: receipts.join(format!("{}-{name}.journal", launch.launch_id)),
                target: ArchiveTarget::ProducerFinished(position),
            };
            Ok(AgentDriverArchiveJobs {
                thread: build("thread-binding", ids.thread, thread_end),
                inputs: build("input-submissions", ids.inputs, input_end),
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}

impl AgentDriverArchiveJobs {
    pub async fn submit(&self, controller: &ArchiveController) -> io::Result<()> {
        for job in [&self.thread, &self.inputs] {
            controller.enqueue(job.clone()).await?;
        }
        Ok(())
    }

    /// Both exact jobs must have durable completion receipts. Producer closure
    /// or successful enqueue alone does not establish remote archive completion.
    pub async fn completion(
        &self,
        controller: &ArchiveController,
    ) -> io::Result<Option<AgentDriverArchiveReceipts>> {
        let Some(thread) = controller.completion(self.thread.job_id).await? else {
            return Ok(None);
        };
        let Some(inputs) = controller.completion(self.inputs.job_id).await? else {
            return Ok(None);
        };
        Ok(Some(AgentDriverArchiveReceipts { thread, inputs }))
    }
}
