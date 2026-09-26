use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use codex_extension_api::ToolExecutionInput;
use codex_extension_api::ToolExecutionKind;
use codex_infra_state::CheckpointKind;
use tokio::sync::Notify;

use crate::ToolOperation;
use crate::WorkspaceCheckpoints;
use crate::WorkspaceLease;
use crate::WorkspaceOperations;

enum CheckpointOwnership {
    Owner,
    Continuation,
}

#[derive(Default)]
struct CompletionState {
    pending: AtomicUsize,
    changed: Notify,
    failure: Mutex<Option<String>>,
}

/// Couples tool admission to one worktree and hands finished operations to
/// checkpoint. Child process recording must use this controller's operations.
#[derive(Clone)]
pub struct ToolWorkspace {
    checkpoints: WorkspaceCheckpoints,
    operations: WorkspaceOperations,
    active: Arc<Mutex<BTreeMap<String, CheckpointOwnership>>>,
    completion: Arc<CompletionState>,
}

impl ToolWorkspace {
    pub fn new(checkpoints: WorkspaceCheckpoints) -> Self {
        Self {
            checkpoints,
            operations: WorkspaceOperations::default(),
            active: Arc::default(),
            completion: Arc::default(),
        }
    }

    pub fn operations(&self) -> WorkspaceOperations {
        self.operations.clone()
    }

    pub(crate) async fn acquire(
        &self,
        input: &ToolExecutionInput<'_>,
        operation: ToolOperation,
    ) -> io::Result<Option<WorkspaceLease>> {
        if input.kind == ToolExecutionKind::Delegating {
            return Ok(None);
        }
        let operation_id = serde_json::to_string(&operation)?;
        let existing = match input.kind {
            ToolExecutionKind::ExistingProcess => input
                .origin
                .map(|origin| self.operations.continue_operation(origin))
                .transpose()?
                .flatten(),
            ToolExecutionKind::Operation | ToolExecutionKind::Delegating => None,
        };
        let (lease, ownership) = match existing {
            Some(lease) => (lease, CheckpointOwnership::Continuation),
            None => (
                self.checkpoints
                    .gate()
                    .acquire(operation_id.clone())
                    .await?,
                CheckpointOwnership::Owner,
            ),
        };
        let mut active = self
            .active
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if active.contains_key(&operation_id) {
            return Err(io::Error::other(
                "tool workspace operation already admitted",
            ));
        }
        self.operations.register(operation, lease.clone())?;
        active.insert(operation_id, ownership);
        // No await between registration and returning the handler share.
        Ok(Some(lease))
    }

    /// Called by the durable finish writer, not its cancellable async waiter.
    /// Scheduling owns the checkpoint share before the writer returns.
    pub(crate) fn finish(&self, operation: &ToolOperation) -> io::Result<()> {
        let operation_id = serde_json::to_string(operation)?;
        let mut active = self
            .active
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let Some(ownership) = active.get(&operation_id) else {
            // Delegation, failed admission, or cancellation before admission.
            return Ok(());
        };
        let lease = self.operations.take(operation)?;
        if matches!(ownership, CheckpointOwnership::Owner) {
            self.completion.pending.fetch_add(1, Ordering::SeqCst);
            let mut job = CheckpointJob {
                completion: Arc::clone(&self.completion),
                finished: false,
            };
            let checkpoints = self.checkpoints.clone();
            tokio::spawn(async move {
                let result = checkpoints
                    .checkpoint(lease, CheckpointKind::Mutation)
                    .await;
                if let Err(error) = result {
                    job.completion
                        .failure
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert_with(|| error.to_string());
                }
                job.finished = true;
                drop(job);
            });
        }
        // A continuation only releases its share; its original owner performs
        // the checkpoint after all process/handler/continuation shares end.
        active.remove(&operation_id);
        Ok(())
    }

    /// After dispatch and audit writes are quiescent, waits for all scheduled
    /// checkpoints, including those whose original tool caller was cancelled.
    pub async fn drain(&self) -> io::Result<()> {
        if !self
            .active
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?
            .is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "tool workspace operations remain active",
            ));
        }
        loop {
            let notified = self.completion.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.completion.pending.load(Ordering::SeqCst) == 0 {
                break;
            }
            notified.await;
        }
        match self
            .completion
            .failure
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?
            .as_ref()
        {
            Some(error) => Err(io::Error::other(error.clone())),
            None => Ok(()),
        }
    }
}

struct CheckpointJob {
    completion: Arc<CompletionState>,
    finished: bool,
}

impl Drop for CheckpointJob {
    fn drop(&mut self) {
        if !self.finished {
            self.completion
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert_with(|| "tool checkpoint task ended before completion".to_owned());
        }
        self.completion.pending.fetch_sub(1, Ordering::SeqCst);
        self.completion.changed.notify_waiters();
    }
}
