use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::path::Path;

use codex_infra_protocol::AgentDescriptor;
use codex_infra_protocol::AgentId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Journal;
use codex_infra_tmux::HostReady;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Serialize, Deserialize)]
struct Launch {
    root: RootSessionId,
    launch_id: MessageId,
    session: String,
    window: String,
    pane: String,
    ready: bool,
}

/// Machine-local readiness for the current process incarnation of each Agent.
/// Saved Ready state becomes usable after the launcher reconciles the live
/// process binding, or the collector observes another Ready from that launch.
pub struct PaneReadiness {
    journal: Journal,
    root: RootSessionId,
    launches: BTreeMap<AgentId, Launch>,
    live: BTreeSet<AgentId>,
}

#[derive(Serialize, Deserialize)]
struct LaunchRecord {
    agent_id: AgentId,
    launch: Option<Launch>,
}

impl PaneReadiness {
    pub fn position(&self) -> codex_infra_state::JournalPosition {
        self.journal.position()
    }

    pub fn open(path: &Path, root: RootSessionId) -> io::Result<Self> {
        let mut launches = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            let record: LaunchRecord =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            match record.launch {
                Some(launch) => {
                    if launch.root != root {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "launch journal belongs to another Root Session",
                        ));
                    }
                    launches.insert(record.agent_id, launch);
                }
                None => {
                    launches.remove(&record.agent_id);
                }
            }
            Ok(())
        })?;
        Ok(Self {
            journal,
            root,
            launches,
            live: BTreeSet::new(),
        })
    }

    /// The launcher calls this with a prepared new binding or a live binding it
    /// reconciled on resume. Repeating the same binding retains persisted Ready.
    pub fn register(&mut self, agent: &AgentDescriptor, launch_id: MessageId) -> io::Result<()> {
        if agent.root_session_id != self.root {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "launch belongs to another Root Session",
            ));
        }
        if self.launches.get(&agent.agent_id).is_some_and(|launch| {
            launch.launch_id == launch_id
                && launch.session == agent.tmux_session
                && launch.window == agent.tmux_window
                && launch.pane == agent.tmux_pane
        }) {
            self.live.insert(agent.agent_id);
            return Ok(());
        }
        let launch = Launch {
            root: agent.root_session_id,
            launch_id,
            session: agent.tmux_session.clone(),
            window: agent.tmux_window.clone(),
            pane: agent.tmux_pane.clone(),
            ready: false,
        };
        self.journal.append(
            &serde_json::to_vec(&LaunchRecord {
                agent_id: agent.agent_id,
                launch: Some(launch.clone()),
            })
            .map_err(io::Error::other)?,
        )?;
        self.launches.insert(agent.agent_id, launch);
        self.live.insert(agent.agent_id);
        Ok(())
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
        if !launch.ready {
            let mut updated = launch.clone();
            updated.ready = true;
            self.journal.append(
                &serde_json::to_vec(&LaunchRecord {
                    agent_id: ready.agent_id,
                    launch: Some(updated.clone()),
                })
                .map_err(io::Error::other)?,
            )?;
            *launch = updated;
        }
        self.live.insert(ready.agent_id);
        Ok(())
    }

    pub fn require_ready(&self, agent: &AgentDescriptor) -> io::Result<()> {
        if self.launches.get(&agent.agent_id).is_some_and(|launch| {
            launch.ready
                && self.live.contains(&agent.agent_id)
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

    pub fn exited(&mut self, agent_id: AgentId) -> io::Result<()> {
        self.journal.append(
            &serde_json::to_vec(&LaunchRecord {
                agent_id,
                launch: None,
            })
            .map_err(io::Error::other)?,
        )?;
        self.launches.remove(&agent_id);
        self.live.remove(&agent_id);
        Ok(())
    }
}
