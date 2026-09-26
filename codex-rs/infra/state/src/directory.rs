use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::path::Path;

use codex_infra_protocol::AgentDescriptor;
use codex_infra_protocol::AgentId;
use codex_infra_protocol::AgentStatus;
use codex_infra_protocol::DirectoryEvent;
use codex_infra_protocol::DirectoryPublisher;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::RootSessionId;
use codex_infra_protocol::TaskId;

use crate::Journal;

#[derive(Default)]
pub struct DirectoryFilter<'a> {
    pub role: Option<&'a str>,
    pub task_id: Option<TaskId>,
    pub machine_id: Option<&'a MachineId>,
    pub status: Option<AgentStatus>,
}

/// Machine streams are appended in source order, including when replicating a
/// remote stream. Queries read the latest observed descriptor and its revision.
pub struct DirectoryStore {
    journal: Journal,
    root_session_id: RootSessionId,
    agents: BTreeMap<AgentId, AgentDescriptor>,
    sources: BTreeMap<MachineId, DirectoryEvent>,
}

impl DirectoryStore {
    pub fn position(&self) -> crate::JournalPosition {
        self.journal.position()
    }

    pub fn open(path: &Path, root_session_id: RootSessionId) -> io::Result<Self> {
        let mut agents = BTreeMap::new();
        let mut sources = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            let event: DirectoryEvent =
                serde_json::from_slice(&record.payload).map_err(io::Error::other)?;
            validate(root_session_id, &agents, &sources, &event)?;
            agents.insert(event.descriptor.agent_id, event.descriptor.clone());
            sources.insert(event.source_machine_id.clone(), event);
            Ok(())
        })?;
        Ok(Self {
            journal,
            root_session_id,
            agents,
            sources,
        })
    }

    pub fn get(&self, agent_id: AgentId) -> Option<&AgentDescriptor> {
        self.agents.get(&agent_id)
    }

    pub fn next_source_sequence(&self, machine_id: &MachineId) -> io::Result<u64> {
        match self.sources.get(machine_id) {
            Some(event) => event
                .source_sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("directory sequence exhausted")),
            None => Ok(0),
        }
    }

    /// Returns false for an already observed source prefix. Missing intermediate
    /// source events are fetched before a later event can advance this view.
    pub fn ingest(&mut self, event: DirectoryEvent) -> io::Result<bool> {
        if event.descriptor.root_session_id != self.root_session_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "directory event belongs to another Root Session",
            ));
        }
        if let Some(previous) = self.sources.get(&event.source_machine_id)
            && event.source_sequence <= previous.source_sequence
        {
            if event.source_sequence == previous.source_sequence && event != *previous {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "directory source rewrote its latest event",
                ));
            }
            return Ok(false);
        }
        validate(self.root_session_id, &self.agents, &self.sources, &event)?;
        self.journal
            .append(&serde_json::to_vec(&event).map_err(io::Error::other)?)?;
        self.agents
            .insert(event.descriptor.agent_id, event.descriptor.clone());
        self.sources.insert(event.source_machine_id.clone(), event);
        Ok(true)
    }

    /// `after` is exclusive; the final returned AgentId is the next page cursor.
    pub fn query(
        &self,
        filter: DirectoryFilter<'_>,
        after: Option<AgentId>,
        limit: NonZeroUsize,
    ) -> Vec<&AgentDescriptor> {
        self.agents
            .range((
                after.map_or(Bound::Unbounded, Bound::Excluded),
                Bound::Unbounded,
            ))
            .map(|(_, agent)| agent)
            .filter(|agent| {
                filter.role.is_none_or(|role| agent.role == role)
                    && filter.task_id.is_none_or(|task| agent.task_id == task)
                    && filter
                        .machine_id
                        .is_none_or(|machine| &agent.machine_id == machine)
                    && filter.status.is_none_or(|status| agent.status == status)
            })
            .take(limit.get())
            .collect()
    }
}

fn validate(
    root: RootSessionId,
    agents: &BTreeMap<AgentId, AgentDescriptor>,
    sources: &BTreeMap<MachineId, DirectoryEvent>,
    event: &DirectoryEvent,
) -> io::Result<()> {
    let next = &event.descriptor;
    let expected_sequence = match sources.get(&event.source_machine_id) {
        Some(previous) => previous.source_sequence.checked_add(1),
        None => Some(0),
    };
    let previous = agents.get(&next.agent_id);
    let expected_revision = match previous {
        Some(previous) => previous.revision.checked_add(1),
        None => Some(0),
    };
    if next.root_session_id != root
        || event.source_machine_id != next.machine_id
        || Some(event.source_sequence) != expected_sequence
        || Some(next.revision) != expected_revision
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "directory identity, sequence or revision mismatch",
        ));
    }
    if let Some(previous) = previous
        && (next.machine_id != previous.machine_id
            || next.parent_agent_id != previous.parent_agent_id)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "directory update changed Agent ownership",
        ));
    }
    let allowed = match (&event.publisher, previous) {
        (DirectoryPublisher::Machine(machine), None) => machine == &next.machine_id,
        (DirectoryPublisher::Machine(machine), Some(previous)) => {
            machine == &next.machine_id
                && next.role == previous.role
                && next.responsibility == previous.responsibility
                && next.task_id == previous.task_id
                && next.repo == previous.repo
                && next.commit == previous.commit
                && next.receives_from == previous.receives_from
                && next.sends_to == previous.sends_to
        }
        (DirectoryPublisher::Agent(agent), Some(previous)) => {
            *agent == next.agent_id
                && next.status == previous.status
                && next.tmux_session == previous.tmux_session
                && next.tmux_window == previous.tmux_window
                && next.tmux_pane == previous.tmux_pane
                && next.tmux_endpoint == previous.tmux_endpoint
        }
        (DirectoryPublisher::Agent(_), None) => false,
    };
    if !allowed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "directory fields do not belong to this publisher",
        ));
    }
    Ok(())
}
