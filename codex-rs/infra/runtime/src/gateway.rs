use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::Path;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::QueueItem;
use codex_infra_state::SpoolQueue;
use codex_infra_tmux::GatewayConnection;
use codex_infra_tmux::GatewayReceiver;
use codex_infra_tmux::GatewaySender;
use codex_infra_tmux::TmuxClient;
use codex_infra_tmux::TransportFrame;

use crate::FrameRouter;
use crate::PaneInputJournal;
use crate::PaneReadiness;

/// One directed machine-pair link. Receipts travel from the receiving host's
/// stdout through its own outbound link, using the same routing as message frames.
/// The runtime schedules independent peers separately and retries durable lanes.
#[derive(Default)]
pub struct PeerLink {
    connection: Option<(SocketAddr, GatewaySender, GatewayReceiver)>,
}

impl PeerLink {
    /// Take ownership during the await so cancellation or any partial write drops
    /// this stream. A subsequent attempt reconnects and writes a whole packet.
    pub async fn forward(
        &mut self,
        endpoint: SocketAddr,
        frame: &TransportFrame,
    ) -> io::Result<()> {
        let (mut sender, receiver) = match self.connection.take() {
            Some((previous, sender, receiver)) if previous == endpoint => (sender, receiver),
            Some(_) | None => GatewayConnection::connect(endpoint).await?.split(),
        };
        sender.send(frame).await?;
        self.connection = Some((endpoint, sender, receiver));
        Ok(())
    }
}

/// Frames received over TCP wait here until their own destination host is ready.
/// Every network occurrence gets a new queue key: replay must reach the host so
/// its inbox can return a receipt that may have been lost on an earlier delivery.
pub struct GatewayInbox {
    root_session_id: RootSessionId,
    queue: SpoolQueue,
}

impl GatewayInbox {
    pub fn open(path: &Path, root_session_id: RootSessionId) -> io::Result<Self> {
        Ok(Self {
            root_session_id,
            queue: SpoolQueue::open(path)?,
        })
    }

    pub fn stage(&mut self, frame: &TransportFrame) -> io::Result<String> {
        let (route, recipient) = match frame {
            TransportFrame::Chunk(chunk) => (&chunk.route, chunk.route.to_agent_id),
            TransportFrame::Receipt(receipt) => (&receipt.route, receipt.route.from_agent_id),
            TransportFrame::Ready(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "host readiness is local to its collector",
                ));
            }
        };
        if route.root_session_id != self.root_session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "gateway frame belongs to another Root Session",
            ));
        }
        let key = MessageId::new().to_string();
        self.queue.enqueue(QueueItem {
            key: key.clone(),
            lane: recipient.to_string(),
            payload: serde_json::to_vec(frame).map_err(io::Error::other)?,
        })?;
        Ok(key)
    }

    pub fn lanes(&self) -> impl Iterator<Item = &str> {
        self.queue.lanes()
    }

    /// The runtime serializes injection per recipient, independently of network
    /// reads. WouldBlock keeps the queued frame available until host readiness.
    pub fn inject_next(
        &mut self,
        agent_id: AgentId,
        router: &FrameRouter<'_>,
        readiness: &PaneReadiness,
        input: &mut PaneInputJournal,
        tmux: &TmuxClient,
    ) -> io::Result<bool> {
        let keys = self.queue.pending_keys(
            &agent_id.to_string(),
            /*first_sequence*/ 0,
            NonZeroUsize::MIN,
        );
        let Some(key) = keys.first() else {
            return Ok(false);
        };
        let item = self.queue.read(key)?;
        let frame: TransportFrame =
            serde_json::from_slice(&item.payload).map_err(io::Error::other)?;
        // Recheck the root when consuming an existing spool after restart.
        if router.root_session_id != self.root_session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "gateway router belongs to another Root Session",
            ));
        }
        input.inject(router, readiness, tmux, &frame)?;
        self.queue.complete(&item.key)?;
        Ok(true)
    }
}
