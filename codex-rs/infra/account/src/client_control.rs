use std::io;
use std::sync::Arc;

use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::RwLock;

use crate::AccountReplicaControl;

/// Tracks owned client exchanges through their final audit write.
#[derive(Clone, Default)]
pub struct AccountClientControl {
    closed: Arc<RwLock<bool>>,
}

impl AccountClientControl {
    pub(crate) async fn begin(&self) -> io::Result<OwnedRwLockReadGuard<bool>> {
        let guard = Arc::clone(&self.closed).read_owned().await;
        if *guard {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "account client has stopped",
            ));
        }
        Ok(guard)
    }

    /// The queued writer blocks new admissions and waits for existing exchanges.
    /// Its owned task continues even when the shutdown waiter is canceled.
    pub async fn stop(&self) -> io::Result<()> {
        let closed = Arc::clone(&self.closed);
        tokio::spawn(async move {
            *closed.write_owned().await = true;
        })
        .await
        .map_err(io::Error::other)
    }
}

#[derive(Clone)]
pub struct AccountRuntimeControl {
    pub(crate) replica: AccountReplicaControl,
    pub(crate) client: AccountClientControl,
}

impl AccountRuntimeControl {
    /// Stops both discovery and client I/O before the host samples account logs.
    pub async fn stop(&self) -> io::Result<()> {
        let control = self.clone();
        tokio::spawn(async move {
            let client = control.client.stop().await;
            let replica = control.replica.stop().await;
            client?;
            replica
        })
        .await
        .map_err(io::Error::other)?
    }
}
