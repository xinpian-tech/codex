use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use codex_infra_protocol::MachineId;
use codex_infra_tmux::TransportFrame;
use tokio::task::Id;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::ControlDispatch;
use crate::FrameRouter;
use crate::PaneReadiness;
use crate::PeerLink;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleOutcome {
    Empty,
    Deferred,
    ReadyObserved,
    Started,
}

#[derive(Debug)]
pub struct ForwardReport {
    pub key: String,
    pub machine_id: MachineId,
    pub endpoint: SocketAddr,
    pub result: io::Result<()>,
}

struct PendingSend {
    key: String,
    machine_id: MachineId,
    endpoint: SocketAddr,
}

/// Services one ControlDispatch spool. The machine loop visits its lanes fairly;
/// slow peers occupy only their own connection and one configured in-flight slot.
/// Dropping this scheduler cancels sends while uncompleted items remain on disk.
pub struct PeerScheduler {
    limit: NonZeroUsize,
    timeout: Duration,
    retry_delay: Duration,
    links: BTreeMap<MachineId, PeerLink>,
    busy: BTreeSet<MachineId>,
    retry_at: BTreeMap<MachineId, Instant>,
    tasks: JoinSet<(PeerLink, io::Result<()>)>,
    pending: BTreeMap<Id, PendingSend>,
    last_lane: Option<String>,
}

impl PeerScheduler {
    pub fn is_idle(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn new(limit: NonZeroUsize, timeout: Duration, retry_delay: Duration) -> Self {
        Self {
            limit,
            timeout,
            retry_delay,
            links: BTreeMap::new(),
            busy: BTreeSet::new(),
            retry_at: BTreeMap::new(),
            tasks: JoinSet::new(),
            pending: BTreeMap::new(),
            last_lane: None,
        }
    }

    /// Rotate the first successful choice between rounds, wrapping once. Unavailable
    /// peers do not stop scheduling other machines. Errors retain their queue
    /// items and are returned for the runtime's event journal.
    pub fn schedule_round(
        &mut self,
        dispatch: &mut ControlDispatch,
        router: &FrameRouter<'_>,
        readiness: &mut PaneReadiness,
    ) -> Vec<(String, io::Error)> {
        let lanes: Vec<String> = dispatch.lanes().map(str::to_owned).collect();
        let split = self
            .last_lane
            .as_ref()
            .map_or(0, |last| lanes.partition_point(|lane| lane <= last));
        let mut errors = Vec::new();
        let mut first_scheduled = None;
        for lane in lanes[split..].iter().chain(&lanes[..split]) {
            match self.schedule_lane(dispatch, lane, router, readiness) {
                Ok(ScheduleOutcome::Started) => {
                    first_scheduled.get_or_insert_with(|| lane.clone());
                }
                Ok(
                    ScheduleOutcome::ReadyObserved
                    | ScheduleOutcome::Empty
                    | ScheduleOutcome::Deferred,
                ) => {}
                Err(error) => errors.push((lane.clone(), error)),
            }
        }
        if let Some(lane) = first_scheduled {
            self.last_lane = Some(lane);
        }
        errors
    }

    pub fn schedule_lane(
        &mut self,
        dispatch: &mut ControlDispatch,
        lane: &str,
        router: &FrameRouter<'_>,
        readiness: &mut PaneReadiness,
    ) -> io::Result<ScheduleOutcome> {
        let Some((key, captured)) = dispatch.next_frame(lane)? else {
            return Ok(ScheduleOutcome::Empty);
        };
        if let TransportFrame::Ready(ready) = &captured.frame {
            readiness.observe(&captured.pane_id, ready)?;
            dispatch.complete(&key)?;
            return Ok(ScheduleOutcome::ReadyObserved);
        }
        let target = router.outbound(&captured.pane_id, &captured.frame)?;
        if self.pending.len() >= self.limit.get()
            || self.busy.contains(&target.machine_id)
            || self
                .retry_at
                .get(&target.machine_id)
                .is_some_and(|deadline| *deadline > Instant::now())
        {
            return Ok(ScheduleOutcome::Deferred);
        }
        let mut link = self.links.remove(&target.machine_id).unwrap_or_default();
        let timeout = self.timeout;
        let endpoint = target.endpoint;
        let handle =
            self.tasks.spawn(async move {
                let result =
                    match tokio::time::timeout(timeout, link.forward(endpoint, &captured.frame))
                        .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "gateway frame forwarding timed out",
                        )),
                    };
                (link, result)
            });
        self.busy.insert(target.machine_id.clone());
        self.pending.insert(
            handle.id(),
            PendingSend {
                key,
                machine_id: target.machine_id,
                endpoint,
            },
        );
        Ok(ScheduleOutcome::Started)
    }

    /// A completed write advances only the gateway queue. HostMailbox retains its
    /// outbound message until the recipient's durable acceptance receipt arrives.
    pub fn finish_next(
        &mut self,
        dispatch: &mut ControlDispatch,
    ) -> io::Result<Option<ForwardReport>> {
        let Some(completion) = self.tasks.try_join_next_with_id() else {
            return Ok(None);
        };
        let (id, link, result) = match completion {
            Ok((id, (link, result))) => (id, link, result),
            Err(error) => (
                error.id(),
                PeerLink::default(),
                Err(io::Error::other(error)),
            ),
        };
        let pending = self
            .pending
            .remove(&id)
            .ok_or_else(|| io::Error::other("forward task binding missing"))?;
        self.busy.remove(&pending.machine_id);
        self.links.insert(pending.machine_id.clone(), link);
        if result.is_ok() {
            self.retry_at.remove(&pending.machine_id);
            dispatch.complete(&pending.key)?;
        } else {
            self.retry_at.insert(
                pending.machine_id.clone(),
                Instant::now() + self.retry_delay,
            );
        }
        Ok(Some(ForwardReport {
            key: pending.key,
            machine_id: pending.machine_id,
            endpoint: pending.endpoint,
            result,
        }))
    }
}
