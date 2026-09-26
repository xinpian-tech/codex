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
use codex_infra_tmux::TmuxClient;
use serde::Serialize;

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
    directory: DirectoryStore,
    readiness: PaneReadiness,
    collector: ControlCollector,
    current_attachment: MessageId,
    attachments: BTreeMap<MessageId, Attachment>,
    reception: GatewayReception,
    ingress: IngressSpool,
    inbox: GatewayInbox,
    injector: InputScheduler,
    observations: Journal,
}

impl TransportSession {
    /// Called inside the machine's Tokio runtime. Agent launch is released only
    /// after the outer actor confirms collector attachment and persists binding.
    pub async fn open(config: TransportSessionConfig, tmux: Arc<TmuxClient>) -> io::Result<Self> {
        fs::create_dir_all(&config.directory)?;
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
        let id = collector
            .directory()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("collector ID missing"))?
            .parse()
            .map_err(io::Error::other)?;
        attachments.insert(id, open_attachment(collector.directory(), id, &config)?);
        Ok(Self {
            config,
            directory,
            readiness,
            collector,
            current_attachment: id,
            attachments,
            reception,
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
        self.readiness.register(agent, launch_id)
    }

    pub fn agent_exited(&mut self, agent_id: AgentId) -> io::Result<()> {
        self.readiness.exited(agent_id)
    }

    /// Each source has a bounded processing slice. Network readers, pane capture,
    /// peer sends and blocking input commands continue in their own workers.
    pub fn tick(&mut self) -> io::Result<()> {
        if self.collector.needs_attention()? {
            return Err(io::Error::other("tmux collector requires reconnection"));
        }
        for _ in 0..self.config.event_batch.get() {
            match self.reception.try_event() {
                Ok(event) => {
                    self.ingress.record(&event)?;
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
                let Some(batch) = attachment.reader.next_batch()? else {
                    source_exhausted = true;
                    break;
                };
                attachment
                    .dispatch
                    .stage(batch, attachment.reader.cursor()?)?;
            }
            while let Some(report) = attachment.sender.finish_next(&mut attachment.dispatch)? {
                record(
                    &mut self.observations,
                    "forward",
                    &report.key,
                    format!("{:?}", report.result),
                )?;
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
                && source_exhausted
                && attachment.dispatch.lanes().next().is_none()
            {
                retired.push(*id);
            }
        }
        for id in retired {
            self.attachments.remove(&id);
        }
        while let Some(report) = self.injector.finish_next(&mut self.inbox)? {
            record(
                &mut self.observations,
                "inject",
                &report.key,
                format!("{:?}", report.result),
            )?;
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
