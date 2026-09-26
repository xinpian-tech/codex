use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use codex_infra_protocol::AgentId;
use codex_infra_tmux::TmuxClient;
use tokio::task::Id;
use tokio::task::JoinSet;

use crate::FrameRouter;
use crate::GatewayInbox;
use crate::PaneInputJournal;
use crate::PaneReadiness;

mod archive;

#[derive(Debug)]
pub struct InjectionReport {
    pub key: String,
    pub agent_id: AgentId,
    pub result: io::Result<u64>,
}

/// Runs blocking tmux commands independently per Agent. The machine finalizer
/// drains completions until `is_idle` before releasing journals or closing panes;
/// blocking commands already running cannot be canceled by dropping a Tokio task.
pub struct InputScheduler {
    directory: PathBuf,
    tmux: Arc<TmuxClient>,
    limit: NonZeroUsize,
    journals: BTreeMap<AgentId, PaneInputJournal>,
    busy: BTreeSet<AgentId>,
    pending: BTreeMap<Id, (AgentId, String)>,
    tasks: JoinSet<(Option<PaneInputJournal>, io::Result<u64>)>,
    last_lane: Option<String>,
}

impl InputScheduler {
    pub fn open(
        directory: PathBuf,
        tmux: Arc<TmuxClient>,
        limit: NonZeroUsize,
    ) -> io::Result<Self> {
        fs::create_dir_all(&directory)?;
        let directory = directory.canonicalize()?;
        Ok(Self {
            directory,
            tmux,
            limit,
            journals: BTreeMap::new(),
            busy: BTreeSet::new(),
            pending: BTreeMap::new(),
            tasks: JoinSet::new(),
            last_lane: None,
        })
    }

    pub fn schedule_round(
        &mut self,
        inbox: &GatewayInbox,
        router: &FrameRouter<'_>,
        readiness: &PaneReadiness,
    ) -> Vec<(String, io::Error)> {
        let lanes: Vec<String> = inbox.lanes().map(str::to_owned).collect();
        let split = self
            .last_lane
            .as_ref()
            .map_or(0, |last| lanes.partition_point(|lane| lane <= last));
        let mut first_scheduled = None;
        let mut errors = Vec::new();
        for lane in lanes[split..].iter().chain(&lanes[..split]) {
            match self.schedule_lane(inbox, lane, router, readiness) {
                Ok(true) => {
                    first_scheduled.get_or_insert_with(|| lane.clone());
                }
                Ok(false) => {}
                Err(error) => errors.push((lane.clone(), error)),
            }
        }
        if let Some(lane) = first_scheduled {
            self.last_lane = Some(lane);
        }
        errors
    }

    /// Returns false when there is no ready input or this Agent/capacity is busy.
    /// Other lanes remain schedulable while this Agent's command is in progress.
    pub fn schedule_lane(
        &mut self,
        inbox: &GatewayInbox,
        lane: &str,
        router: &FrameRouter<'_>,
        readiness: &PaneReadiness,
    ) -> io::Result<bool> {
        if self.pending.len() >= self.limit.get() {
            return Ok(false);
        }
        let prepared = match inbox.prepare_input(lane, router, readiness) {
            Ok(Some(prepared)) => prepared,
            Ok(None) => return Ok(false),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(error),
        };
        let agent_id = prepared.target.agent_id;
        if self.busy.contains(&agent_id) {
            return Ok(false);
        }
        let previous = self.journals.remove(&agent_id);
        let path = self.directory.join(format!("{agent_id}.journal"));
        let tmux = Arc::clone(&self.tmux);
        let handle = self.tasks.spawn_blocking(move || {
            let mut journal = match previous.map_or_else(|| PaneInputJournal::open(&path), Ok) {
                Ok(journal) => journal,
                Err(error) => return (None, Err(error)),
            };
            let result = journal.inject_resolved(&prepared.target, &tmux, &prepared.frame);
            // A journal append error requires reopening before another attempt.
            let reusable = result.is_ok().then_some(journal);
            (reusable, result)
        });
        self.busy.insert(agent_id);
        self.pending.insert(handle.id(), (agent_id, prepared.key));
        Ok(true)
    }

    pub fn is_idle(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn finish_next(&mut self, inbox: &mut GatewayInbox) -> io::Result<Option<InjectionReport>> {
        let Some(completion) = self.tasks.try_join_next_with_id() else {
            return Ok(None);
        };
        let (id, journal, result) = match completion {
            Ok((id, (journal, result))) => (id, journal, result),
            Err(error) => (error.id(), None, Err(io::Error::other(error))),
        };
        let (agent_id, key) = self
            .pending
            .remove(&id)
            .ok_or_else(|| io::Error::other("input task binding missing"))?;
        self.busy.remove(&agent_id);
        if let Some(journal) = journal {
            self.journals.insert(agent_id, journal);
        }
        if result.is_ok() {
            inbox.complete_input(&key)?;
        }
        Ok(Some(InjectionReport {
            key,
            agent_id,
            result,
        }))
    }
}
