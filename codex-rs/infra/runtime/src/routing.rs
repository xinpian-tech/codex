use std::io;
use std::net::SocketAddr;

use codex_infra_protocol::AgentDescriptor;
use codex_infra_protocol::AgentId;
use codex_infra_protocol::FrameRoute;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::DirectoryStore;
use codex_infra_tmux::TransportFrame;

#[derive(Debug, Clone)]
pub struct ForwardTarget {
    pub agent_id: AgentId,
    pub machine_id: MachineId,
    pub endpoint: SocketAddr,
    pub directory_revision: u64,
}

/// Resolves transport metadata after the sender has selected an explicit Agent.
/// Role/Task selection belongs to the Agent's tools; this layer never reads body
/// text or chooses a substitute recipient when a destination is unavailable.
pub struct FrameRouter<'a> {
    pub root_session_id: RootSessionId,
    pub machine_id: &'a MachineId,
    pub directory: &'a DirectoryStore,
}

impl FrameRouter<'_> {
    /// Local recipients also return a TCP endpoint, following the same path as
    /// remote recipients. Receipt direction is the reverse of the original route.
    pub fn outbound(&self, pane_id: &str, frame: &TransportFrame) -> io::Result<ForwardTarget> {
        let (route, sender, recipient) = addresses(frame)?;
        let source = self.lookup(route, sender)?;
        if &source.machine_id != self.machine_id || source.tmux_pane != pane_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame did not originate in its sender pane",
            ));
        }
        let target = self.lookup(route, recipient)?;
        Ok(ForwardTarget {
            agent_id: recipient,
            machine_id: target.machine_id.clone(),
            endpoint: target.tmux_endpoint,
            directory_revision: target.revision,
        })
    }

    /// Resolves the current pane after network receipt. The caller journals the
    /// target and exact input bytes before invoking tmux's input command, and
    /// holds pending input until this host has completed its raw-mode handshake.
    pub fn incoming(&self, frame: &TransportFrame) -> io::Result<&AgentDescriptor> {
        let (route, _, recipient) = addresses(frame)?;
        let target = self.lookup(route, recipient)?;
        if &target.machine_id != self.machine_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame destination is on another machine",
            ));
        }
        Ok(target)
    }

    fn lookup(&self, route: &FrameRoute, agent_id: AgentId) -> io::Result<&AgentDescriptor> {
        if route.root_session_id != self.root_session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame belongs to another Root Session",
            ));
        }
        let agent = self.directory.get(agent_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "Agent is not yet in the local directory view",
            )
        })?;
        if agent.root_session_id != self.root_session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "directory belongs to another Root Session",
            ));
        }
        Ok(agent)
    }
}

fn addresses(frame: &TransportFrame) -> io::Result<(&FrameRoute, AgentId, AgentId)> {
    Ok(match frame {
        TransportFrame::Chunk(chunk) => (
            &chunk.route,
            chunk.route.from_agent_id,
            chunk.route.to_agent_id,
        ),
        TransportFrame::Receipt(receipt) => (
            &receipt.route,
            receipt.route.to_agent_id,
            receipt.route.from_agent_id,
        ),
        TransportFrame::Ready(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host readiness is consumed by the local runtime",
            ));
        }
    })
}
