use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use codex_extension_api::ToolExecutionLease;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// One worktree's mutation boundary, created by its checkpoint service.
#[derive(Clone)]
pub struct WorkspaceGate {
    semaphore: Arc<Semaphore>,
    failure: Arc<Mutex<Option<String>>>,
}

pub(crate) enum GateReadiness {
    Ready,
    RecoveryRequired(String),
}

enum Admission {
    Mutation,
    Recovery,
}

impl WorkspaceGate {
    pub(crate) fn new(readiness: GateReadiness) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(/*permits*/ 1)),
            failure: Arc::new(Mutex::new(match readiness {
                GateReadiness::Ready => None,
                GateReadiness::RecoveryRequired(reason) => Some(reason),
            })),
        }
    }

    /// The controller retains one share for checkpoint handoff; handlers and
    /// child processes retain clones until they have stopped writing.
    pub async fn acquire(&self, operation_id: String) -> io::Result<WorkspaceLease> {
        self.acquire_inner(operation_id, Admission::Mutation).await
    }

    /// Acquires exclusivity for retrying the service's pending checkpoint.
    /// Recovery remains available while new mutation admission is suspended.
    pub async fn acquire_recovery(&self, operation_id: String) -> io::Result<WorkspaceLease> {
        self.acquire_inner(operation_id, Admission::Recovery).await
    }

    async fn acquire_inner(
        &self,
        operation_id: String,
        admission: Admission,
    ) -> io::Result<WorkspaceLease> {
        let permit = Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .map_err(io::Error::other)?;
        // Check after acquiring the permit, including requests queued before
        // the preceding checkpoint failed. The failure is set before release.
        if matches!(admission, Admission::Mutation)
            && let Some(error) = self
                .failure
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?
                .as_ref()
        {
            return Err(io::Error::other(format!(
                "workspace checkpoint requires recovery: {error}"
            )));
        }
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
        let failure = Arc::clone(&self.failure);
        tokio::spawn(async move {
            let lease = lease.into_exclusive().await?;
            tokio::task::spawn_blocking(move || {
                let mut attempt = CheckpointExecution {
                    lease,
                    failure,
                    finished: false,
                };
                let result = operation(&attempt.lease.operation_id);
                *attempt
                    .failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    result.as_ref().err().map(ToString::to_string);
                attempt.finished = true;
                drop(attempt);
                result
            })
            .await
            .map_err(io::Error::other)?
        })
        .await
        .map_err(io::Error::other)?
    }
}

struct CheckpointExecution {
    lease: LeaseState,
    failure: Arc<Mutex<Option<String>>>,
    finished: bool,
}

impl Drop for CheckpointExecution {
    fn drop(&mut self) {
        if !self.finished {
            // Record unwinding before dropping the lease's semaphore permit.
            *self
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some("checkpoint execution ended before completion".to_owned());
        }
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
