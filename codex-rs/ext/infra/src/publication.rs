use std::io;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MessageId;
use codex_infra_runtime::HostMailbox;
use codex_infra_state::CheckpointKind;
use codex_infra_state::CheckpointPhase;

use crate::WorkspaceCheckpoints;

impl WorkspaceCheckpoints {
    /// Publishes a new envelope while retaining worktree exclusivity from
    /// commit/push through durable outbox enqueue and terminal output. Call
    /// outside a tool's mutation lease. Background processes must release their
    /// shares first; process-control results do not use this publication path.
    ///
    /// Cancellation stops waiting, not the owned checkpoint/publication job.
    /// Once queued, retries use HostMailbox replay with the original envelope.
    /// Final results additionally require the finalizer's archive watermark.
    pub async fn send_checkpointed<W: Write + Send + 'static>(
        &self,
        message: AgentMessage,
        mailbox: Arc<Mutex<HostMailbox<W>>>,
    ) -> io::Result<u64> {
        self.context.validate_message_source(&message)?;
        let checkpoints = self.clone();
        tokio::spawn(async move {
            // A fresh attempt must inspect Git again even if a previous send
            // failed before enqueue and the caller reused its message ID.
            let operation_id = format!("message-{}-{}", message.message_id, MessageId::new());
            let lease = checkpoints.gate.acquire(operation_id).await?;
            let state = Arc::clone(&checkpoints.state);
            let context = Arc::clone(&checkpoints.context);
            checkpoints
                .gate
                .run(lease, move |operation_id| {
                    let already_queued = mailbox
                        .lock()
                        .map_err(|error| io::Error::other(error.to_string()))?
                        .outbound(message.message_id)
                        .is_some();
                    if already_queued {
                        // This is an enqueue error, not a failed Git checkpoint.
                        return Ok(Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "message already queued; replay its original envelope",
                        )));
                    }
                    let record = state
                        .lock()
                        .map_err(|error| io::Error::other(error.to_string()))?
                        .complete(&context, operation_id, CheckpointKind::Mutation)?;
                    let CheckpointPhase::Completed { receipt, .. } = record.phase else {
                        return Err(io::Error::other("message checkpoint is not complete"));
                    };
                    // Preserve the successful checkpoint if terminal output fails.
                    // The durable outbox owns replay after a partial write.
                    Ok(mailbox
                        .lock()
                        .map_err(|error| io::Error::other(error.to_string()))
                        .and_then(|mut mailbox| mailbox.send_checkpointed(message, &receipt)))
                })
                .await?
        })
        .await
        .map_err(io::Error::other)?
    }
}
