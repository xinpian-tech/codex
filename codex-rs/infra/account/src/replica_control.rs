use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::oneshot;
use tokio::sync::watch;

/// Shared shutdown boundary for an account replica retained by erased auth
/// providers. Completion follows worker exit, including its active blocking job.
#[derive(Clone)]
pub struct AccountReplicaControl {
    pub(super) stop: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    pub(super) completion: watch::Receiver<Option<Result<(), String>>>,
}

impl AccountReplicaControl {
    pub(super) fn request_stop(&self) -> io::Result<()> {
        let mut stop = self
            .stop
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if let Some(stop) = stop.take() {
            let _ = stop.send(());
        }
        Ok(())
    }

    /// Repeated callers observe the same terminal result. Canceling a waiter
    /// leaves shutdown and the completion observer running independently.
    pub async fn stop(&self) -> io::Result<()> {
        self.request_stop()?;
        let mut completion = self.completion.clone();
        loop {
            if let Some(result) = completion.borrow_and_update().clone() {
                return result.map_err(io::Error::other);
            }
            completion.changed().await.map_err(|_| {
                io::Error::other("account replica exited without a completion result")
            })?;
        }
    }
}
