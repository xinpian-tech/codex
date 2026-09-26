use std::io;

use tokio::sync::oneshot;

use super::InProcessClientHandle;
use super::InProcessClientMessage;

impl InProcessClientHandle {
    /// Stops request processing and waits for background/thread shutdown with
    /// no timeout-based abort. Acknowledgement and worker errors propagate to
    /// the managed host. Producer-specific output/archive drain remains the
    /// host's responsibility; this does not prove all detached work has ended.
    pub async fn shutdown_drained(self) -> io::Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        let sent = self
            .client
            .client_tx
            .send(InProcessClientMessage::ShutdownDrained { done_tx })
            .await;
        let result = match sent {
            Ok(()) => done_rx
                .await
                .map_err(io::Error::other)
                .and_then(|result| result),
            Err(_) => Err(io::Error::other(
                "runtime stopped before drained shutdown request",
            )),
        };
        let joined = self.runtime_handle.await.map_err(io::Error::other);
        result.and(joined)
    }
}
