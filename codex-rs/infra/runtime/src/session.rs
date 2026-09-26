use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::DirectoryEvent;
use codex_infra_protocol::DirectoryPublisher;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::DirectoryFilter;
use codex_infra_state::DirectoryStore;
use codex_infra_state::Journal;
use codex_infra_tmux::GatewayListener;
use codex_infra_tmux::PaneProcessState;
use codex_infra_tmux::TmuxClient;
use serde::Serialize;
use tokio::task::Id;
use tokio::task::JoinSet;

use crate::CollectorFinished;
use crate::ControlCollector;
use crate::ControlDispatch;
use crate::ControlFrameReader;
use crate::FrameRouter;
use crate::GatewayInbox;
use crate::GatewayReception;
use crate::IngressSpool;
use crate::InputScheduler;
use crate::PaneReadiness;
use crate::PeerScheduler;
use crate::ReceptionEvent;

mod archive;
mod archive_binding;
mod archive_jobs;
mod collector;
mod shutdown;
mod tails;
use archive_binding::TransportArchiveBinding;
pub use archive_jobs::TransportArchiveJobIds;
pub use archive_jobs::TransportArchiveJobs;
use collector::CollectorOwner;
pub use tails::CaptureTailEntry;
pub use tails::CaptureTailPage;
pub use tails::CaptureTailState;

pub struct TransportSessionConfig {
    pub directory: PathBuf,
    pub root_session_id: RootSessionId,
    pub machine_id: MachineId,
    pub bind_address: IpAddr,
    pub event_batch: NonZeroUsize,
    pub receive_capacity: NonZeroUsize,
    pub send_concurrency: NonZeroUsize,
    pub input_concurrency: NonZeroUsize,
    pub send_timeout: Duration,
    pub retry_delay: Duration,
}

struct Attachment {
    source: PathBuf,
    reader: ControlFrameReader,
    dispatch: ControlDispatch,
    sender: PeerScheduler,
}

#[derive(Serialize)]
struct TransportObservation<'a> {
    operation: &'a str,
    key: &'a str,
    outcome: String,
}

/// The transport portion of one machine's Root Session. It preserves old
/// collector attachments and drives their durable backlogs alongside live input.
/// The outer machine actor serializes directory/launch updates with `tick`.
pub struct TransportSession {
    config: TransportSessionConfig,
    archive_binding: TransportArchiveBinding,
    directory: DirectoryStore,
    readiness: PaneReadiness,
    collector: CollectorOwner,
    tmux: Arc<TmuxClient>,
    retiring: BTreeMap<Id, MessageId>,
    reapers: JoinSet<String>,
    current_attachment: MessageId,
    attachments: BTreeMap<MessageId, Attachment>,
    reception: GatewayReception,
    pending_reception: Option<ReceptionEvent>,
    ingress: IngressSpool,
    inbox: GatewayInbox,
    injector: InputScheduler,
    observations: Journal,
}

impl TransportSession {
    /// Called inside the machine's Tokio runtime. Agent launch is released only
    /// after the outer actor confirms collector attachment and persists binding.
    pub async fn open(
        mut config: TransportSessionConfig,
        tmux: Arc<TmuxClient>,
    ) -> io::Result<Self> {
        fs::create_dir_all(&config.directory)?;
        config.directory = config.directory.canonicalize()?;
        let archive_binding = TransportArchiveBinding::open(&config)?;
        let mut directory = DirectoryStore::open(
            &config.directory.join("directory.journal"),
            config.root_session_id,
        )?;
        let ingress = IngressSpool::open(
            &config.directory.join("network.journal"),
            &config.directory.join("network-cursor.journal"),
        )?;
        let inbox = GatewayInbox::open(
            &config.directory.join("network-inbox.journal"),
            config.root_session_id,
        )?;
        let injector = InputScheduler::open(
            config.directory.join("pane-input"),
            Arc::clone(&tmux),
            config.input_concurrency,
        )?;
        let observations = Journal::open(&config.directory.join("transport.journal"), |_| Ok(()))?;
        let readiness = PaneReadiness::open(
            &config.directory.join("launches.journal"),
            config.root_session_id,
        )?;
        let listener = GatewayListener::bind(config.bind_address).await?;
        let reception = GatewayReception::start(listener, config.receive_capacity)?;
        let endpoint = reception.endpoint();
        let updated_at = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(io::Error::other)?
                .as_secs(),
        )
        .map_err(io::Error::other)?;
        let mut after = None;
        loop {
            let page: Vec<_> = directory
                .query(
                    DirectoryFilter {
                        machine_id: Some(&config.machine_id),
                        ..DirectoryFilter::default()
                    },
                    after,
                    config.event_batch,
                )
                .into_iter()
                .cloned()
                .collect();
            if page.is_empty() {
                break;
            }
            for mut descriptor in page {
                after = Some(descriptor.agent_id);
                if descriptor.tmux_endpoint == endpoint {
                    continue;
                }
                descriptor.tmux_endpoint = endpoint;
                descriptor.revision = descriptor
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("directory revision exhausted"))?;
                descriptor.updated_at = updated_at;
                directory.ingest(DirectoryEvent {
                    source_machine_id: config.machine_id.clone(),
                    publisher: DirectoryPublisher::Machine(config.machine_id.clone()),
                    source_sequence: directory.next_source_sequence(&config.machine_id)?,
                    descriptor,
                })?;
            }
        }
        let collectors = config.directory.join("collectors");
        fs::create_dir_all(&collectors)?;
        let mut attachments = BTreeMap::new();
        for entry in fs::read_dir(&collectors)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() && entry.path().join("stdout.journal").is_file() {
                let id = entry
                    .file_name()
                    .to_str()
                    .ok_or_else(|| io::Error::other("collector ID is not UTF-8"))?
                    .parse()
                    .map_err(io::Error::other)?;
                attachments.insert(id, open_attachment(&entry.path(), id, &config)?);
            }
        }
        let session = tmux.ensure_session(config.root_session_id)?;
        let collector = ControlCollector::attach(&tmux, &session, &collectors)?;
        let id = collector.attachment_id();
        attachments.insert(id, open_attachment(collector.directory(), id, &config)?);
        Ok(Self {
            config,
            archive_binding,
            directory,
            readiness,
            collector: CollectorOwner::Running(Box::new(collector)),
            tmux,
            retiring: BTreeMap::new(),
            reapers: JoinSet::new(),
            current_attachment: id,
            attachments,
            reception,
            pending_reception: None,
            ingress,
            inbox,
            injector,
            observations,
        })
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.reception.endpoint()
    }

    pub fn update_directory(&mut self, event: DirectoryEvent) -> io::Result<bool> {
        self.directory.ingest(event)
    }

    pub fn register_launch(&mut self, agent_id: AgentId, launch_id: MessageId) -> io::Result<()> {
        let agent = self.directory.get(agent_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Agent directory entry missing")
        })?;
        if agent.machine_id != self.config.machine_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "launch belongs to another machine",
            ));
        }
        let process = self
            .tmux
            .inspect_launch(&agent.tmux_session, agent_id, launch_id)?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "launch process missing from tmux")
            })?;
        if process.state != PaneProcessState::Alive
            || process.placement.pane_id != agent.tmux_pane
            || process.placement.window_id != agent.tmux_window
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "live launch does not match Agent placement",
            ));
        }
        self.readiness.register(agent, launch_id)
    }

    pub fn agent_exited(&mut self, agent_id: AgentId) -> io::Result<()> {
        self.readiness.exited(agent_id)
    }

    /// Each source has a bounded processing slice. Network readers, pane capture,
    /// peer sends and blocking input commands continue in their own workers.
    pub fn tick(&mut self) -> io::Result<()> {
        let collector = self.collector.running()?;
        if collector.needs_attention()? {
            let session = self.tmux.ensure_session(self.config.root_session_id)?;
            let replacement = ControlCollector::attach(
                &self.tmux,
                &session,
                &self.config.directory.join("collectors"),
            )?;
            let id = replacement.attachment_id();
            let attachment = open_attachment(replacement.directory(), id, &self.config)?;
            let mut previous = std::mem::replace(collector, replacement);
            let previous_id = std::mem::replace(&mut self.current_attachment, id);
            self.attachments.insert(id, attachment);
            let handle = self.reapers.spawn_blocking(move || {
                let detached = previous.detach();
                let completed = previous.finish();
                format!("detach={detached:?}; finish={completed:?}")
            });
            self.retiring.insert(handle.id(), previous_id);
            record(
                &mut self.observations,
                "collector_reconnected",
                &id.to_string(),
                format!("previous={previous_id}"),
            )?;
        }
        self.finish_work()?;
        for _ in 0..self.config.event_batch.get() {
            self.record_pending_reception()?;
            match self.reception.try_event() {
                Ok(event) => {
                    self.pending_reception = Some(event);
                    self.record_pending_reception()?;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    return Err(io::Error::other("gateway reception stopped"));
                }
            }
        }
        for _ in 0..self.config.event_batch.get() {
            if self.ingress.advance_one(&mut self.inbox)?.is_none() {
                break;
            }
        }
        let router = FrameRouter {
            root_session_id: self.config.root_session_id,
            machine_id: &self.config.machine_id,
            directory: &self.directory,
        };
        let mut retired = Vec::new();
        for (id, attachment) in &mut self.attachments {
            let mut source_exhausted = false;
            for _ in 0..self.config.event_batch.get() {
                if !attachment.stage_next()? {
                    source_exhausted = true;
                    break;
                }
            }
            for (lane, error) in attachment.sender.schedule_round(
                &mut attachment.dispatch,
                &router,
                &mut self.readiness,
            ) {
                record(
                    &mut self.observations,
                    "schedule_forward",
                    &lane,
                    error.to_string(),
                )?;
            }
            if *id != self.current_attachment
                && !self.retiring.values().any(|retiring| retiring == id)
                && source_exhausted
                && attachment.dispatch.lanes().next().is_none()
                && let Some(completed) = CollectorFinished::read(
                    &self
                        .config
                        .directory
                        .join("collectors")
                        .join(id.to_string()),
                    *id,
                )?
                && attachment.reader.cursor()?.source_position() == completed.stdout_position
            {
                retired.push(*id);
            }
        }
        for id in retired {
            self.attachments.remove(&id);
        }
        for (lane, error) in self
            .injector
            .schedule_round(&self.inbox, &router, &self.readiness)
        {
            record(
                &mut self.observations,
                "schedule_input",
                &lane,
                error.to_string(),
            )?;
        }
        Ok(())
    }

    /// Stop scheduling new work and wait for existing commands to finish. The
    /// collector remains alive; ownership can be handed back to a machine actor
    /// without closing Agent panes or discarding unconsumed source journals.
    pub async fn drain_in_flight(&mut self, poll_interval: Duration) -> io::Result<()> {
        if poll_interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "drain interval must be positive",
            ));
        }
        loop {
            self.finish_work()?;
            if self.injector.is_idle()
                && self.reapers.is_empty()
                && self
                    .attachments
                    .values()
                    .all(|attachment| attachment.sender.is_idle())
            {
                return Ok(());
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    fn finish_work(&mut self) -> io::Result<()> {
        while let Some(completion) = self.reapers.try_join_next_with_id() {
            let (task_id, outcome) = match completion {
                Ok((task_id, outcome)) => (task_id, outcome),
                Err(error) => (error.id(), error.to_string()),
            };
            let id = self
                .retiring
                .remove(&task_id)
                .ok_or_else(|| io::Error::other("collector reaper binding missing"))?;
            record(
                &mut self.observations,
                "collector_retired",
                &id.to_string(),
                outcome,
            )?;
        }
        for attachment in self.attachments.values_mut() {
            while let Some(report) = attachment.sender.finish_next(&mut attachment.dispatch)? {
                record(
                    &mut self.observations,
                    "forward",
                    &report.key,
                    format!("{:?}", report.result),
                )?;
            }
        }
        while let Some(report) = self.injector.finish_next(&mut self.inbox)? {
            record(
                &mut self.observations,
                "inject",
                &report.key,
                format!("{:?}", report.result),
            )?;
        }
        Ok(())
    }
}

fn open_attachment(
    path: &Path,
    id: MessageId,
    config: &TransportSessionConfig,
) -> io::Result<Attachment> {
    let dispatch = ControlDispatch::open(
        &path.join("dispatch.journal"),
        &path.join("dispatch-cursor.journal"),
        id,
    )?;
    let reader = ControlFrameReader::open(&path.join("stdout.journal"), dispatch.cursor())?;
    let sender = PeerScheduler::new(
        config.send_concurrency,
        config.send_timeout,
        config.retry_delay,
    );
    Ok(Attachment {
        source: path.join("stdout.journal"),
        reader,
        dispatch,
        sender,
    })
}

fn record(journal: &mut Journal, operation: &str, key: &str, outcome: String) -> io::Result<()> {
    journal.append(
        &serde_json::to_vec(&TransportObservation {
            operation,
            key,
            outcome,
        })
        .map_err(io::Error::other)?,
    )?;
    Ok(())
}
