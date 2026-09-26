use std::io;
use std::sync::Arc;

use codex_extension_api::ToolExecutionLease;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// One worktree's mutation boundary, created by its checkpoint service.
#[derive(Clone)]
pub struct WorkspaceGate {
    semaphore: Arc<Semaphore>,
}

impl WorkspaceGate {
    pub(crate) fn new() -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(/*permits*/ 1)),
        }
    }

    /// The controller retains one share for checkpoint handoff; handlers and
    /// child processes retain clones until they have stopped writing.
    pub async fn acquire(&self, operation_id: String) -> io::Result<WorkspaceLease> {
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .map_err(io::Error::other)?;
        Ok(WorkspaceLease {
            inner: Some(Arc::new(LeaseState {
                gate: Arc::clone(&self.semaphore),
                _permit: permit,
                operation_id,
                changed: Arc::new(Notify::new()),
            })),
        })
    }

    pub(crate) async fn run<T: Send + 'static>(
        &self,
        lease: WorkspaceLease,
        operation: impl FnOnce(&str) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let inner = lease
            .inner
            .as_ref()
            .ok_or_else(|| io::Error::other("workspace lease consumed"))?;
        if !Arc::ptr_eq(&self.semaphore, &inner.gate) {
            return Err(io::Error::other(
                "checkpoint belongs to a different workspace gate",
            ));
        }
        // Own the handoff before the first wait. Cancelling the caller neither
        // releases exclusivity nor cancels a commit/push already in flight.
        tokio::spawn(async move {
            let lease = lease.into_exclusive().await?;
            tokio::task::spawn_blocking(move || {
                let result = operation(&lease.operation_id);
                drop(lease);
                result
            })
            .await
            .map_err(io::Error::other)?
        })
        .await
        .map_err(io::Error::other)?
    }
}

struct LeaseState {
    gate: Arc<Semaphore>,
    _permit: OwnedSemaphorePermit,
    operation_id: String,
    changed: Arc<Notify>,
}

/// A share of the current write operation. Cloning retains the same operation,
/// rather than admitting another writer. The controller passes its share into
/// checkpoint after arranging for all handler/process shares to be released.
#[derive(Clone)]
pub struct WorkspaceLease {
    inner: Option<Arc<LeaseState>>,
}

impl ToolExecutionLease for WorkspaceLease {}

impl WorkspaceLease {
    async fn into_exclusive(mut self) -> io::Result<LeaseState> {
        loop {
            let inner = self
                .inner
                .as_ref()
                .ok_or_else(|| io::Error::other("workspace lease consumed"))?;
            let changed = Arc::clone(&inner.changed);
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if Arc::strong_count(inner) == 1 {
                break;
            }
            notified.await;
        }
        let inner = self
            .inner
            .take()
            .ok_or_else(|| io::Error::other("workspace lease consumed"))?;
        Arc::try_unwrap(inner).map_err(|_| io::Error::other("workspace lease still shared"))
    }
}

impl Drop for WorkspaceLease {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let changed = Arc::clone(&inner.changed);
            // Decrement before notifying, so a woken controller can observe
            // that the last writer has actually released its share.
            drop(inner);
            changed.notify_waiters();
        }
    }
}
