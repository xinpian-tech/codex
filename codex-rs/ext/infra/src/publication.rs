use std::io;
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MessageId;
use codex_infra_runtime::HostMailbox;
use codex_infra_state::CheckpointKind;
use codex_infra_state::CheckpointPhase;

use crate::WorkspaceCheckpoints;

mod tasks;
pub(super) use tasks::PublicationTasks;

impl WorkspaceCheckpoints {
    /// Closes new publication admission and waits for all admitted jobs,
    /// including those whose callers stopped waiting. Call after the final
    /// result has been admitted, while checkpoint and terminal services remain
    /// available. Pending disk intents and remote delivery still require their
    /// own recovery/outbox reconciliation.
    pub async fn shutdown_publications(&self) -> io::Result<()> {
        self.publications.shutdown().await
    }

    /// Resumes a bounded batch after workspace operation recovery. A crash
    /// after outbox enqueue reuses that envelope; an unqueued intent receives
    /// a fresh checkpoint. Run the outbox replay protocol for completed intents
    /// whose remote acknowledgement is still outstanding.
    pub async fn recover_publications<W: Write + Send + 'static>(
        &self,
        mailbox: Arc<Mutex<HostMailbox<W>>>,
        limit: NonZeroUsize,
    ) -> io::Result<usize> {
        let reader = Arc::clone(&mailbox);
        let pending = tokio::task::spawn_blocking(move || {
            reader
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?
                .pending_publications(limit)
        })
        .await
        .map_err(io::Error::other)??;
        let count = pending.len();
        for message in pending {
            self.send_checkpointed(message, Arc::clone(&mailbox))
                .await?;
        }
        Ok(count)
    }

    /// Publishes a new envelope while retaining worktree exclusivity from
    /// commit/push through durable outbox enqueue and terminal output. Call
    /// outside a tool's mutation lease. Background processes must release their
    /// shares first; process-control results do not use this publication path.
    ///
    /// Intent is persisted before waiting for the worktree. Cancellation stops
    /// waiting, not the owned checkpoint/publication job. On restart, submit
    /// HostMailbox::pending_publications after recovering workspace operations.
    /// Once queued, retries use HostMailbox replay with the original envelope.
    /// Final results additionally require the finalizer's archive watermark.
    pub async fn send_checkpointed<W: Write + Send + 'static>(
        &self,
        message: AgentMessage,
        mailbox: Arc<Mutex<HostMailbox<W>>>,
    ) -> io::Result<u64> {
        self.context.validate_message_source(&message)?;
        let checkpoints = self.clone();
        self.publications
            .spawn(async move {
                let staging = Arc::clone(&mailbox);
                let draft = message.clone();
                tokio::task::spawn_blocking(move || {
                    staging
                        .lock()
                        .map_err(|error| io::Error::other(error.to_string()))?
                        .stage_publication(&draft)
                })
                .await
                .map_err(io::Error::other)??;
                // A fresh attempt must inspect Git again even if a previous send
                // failed before enqueue and the caller reused its message ID.
                let operation_id = format!("message-{}-{}", message.message_id, MessageId::new());
                let lease = checkpoints.gate.acquire(operation_id).await?;
                let state = Arc::clone(&checkpoints.state);
                let context = Arc::clone(&checkpoints.context);
                checkpoints
                    .gate
                    .run(lease, move |operation_id| {
                        {
                            let mut mailbox = mailbox
                                .lock()
                                .map_err(|error| io::Error::other(error.to_string()))?;
                            if let Some(sequence) =
                                mailbox.reconcile_publication(message.message_id)?
                            {
                                return Ok(mailbox
                                    .replay_message(message.message_id)
                                    .map(|()| sequence));
                            }
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
                            .and_then(|mut mailbox| {
                                let message_id = message.message_id;
                                let sent = mailbox.send_checkpointed(message, &receipt);
                                // Enqueue may have succeeded even if terminal output
                                // failed. Preserve its identity for outbox replay.
                                mailbox.reconcile_publication(message_id)?;
                                sent
                            }))
                    })
                    .await?
            })?
            .await
            .map_err(io::Error::other)?
    }
}
