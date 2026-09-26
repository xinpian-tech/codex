use std::collections::BTreeMap;
use std::io;

use codex_infra_protocol::AgentDescriptor;
use codex_infra_protocol::AgentId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_tmux::HostReady;

struct Launch {
    root: RootSessionId,
    launch_id: MessageId,
    session: String,
    window: String,
    pane: String,
    ready: bool,
}

/// Machine-local readiness for the current process incarnation of each Agent.
/// The launch ID is persisted with process bindings; restoring registration starts
/// unready until the collector observes that incarnation's Ready frame again.
#[derive(Default)]
pub struct PaneReadiness {
    launches: BTreeMap<AgentId, Launch>,
}

impl PaneReadiness {
    pub fn register(&mut self, agent: &AgentDescriptor, launch_id: MessageId) {
        self.launches.insert(
            agent.agent_id,
            Launch {
                root: agent.root_session_id,
                launch_id,
                session: agent.tmux_session.clone(),
                window: agent.tmux_window.clone(),
                pane: agent.tmux_pane.clone(),
                ready: false,
            },
        );
    }

    pub fn observe(&mut self, pane_id: &str, ready: &HostReady) -> io::Result<()> {
        let launch = self
            .launches
            .get_mut(&ready.agent_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "host launch not registered"))?;
        if launch.root != ready.root_session_id
            || launch.launch_id != ready.launch_id
            || launch.pane != pane_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "host readiness differs from current launch",
            ));
        }
        launch.ready = true;
        Ok(())
    }

    pub fn require_ready(&self, agent: &AgentDescriptor) -> io::Result<()> {
        if self.launches.get(&agent.agent_id).is_some_and(|launch| {
            launch.ready
                && launch.root == agent.root_session_id
                && launch.session == agent.tmux_session
                && launch.window == agent.tmux_window
                && launch.pane == agent.tmux_pane
        }) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "destination host has not announced readiness",
            ))
        }
    }

    pub fn exited(&mut self, agent_id: AgentId) {
        self.launches.remove(&agent_id);
    }
}
