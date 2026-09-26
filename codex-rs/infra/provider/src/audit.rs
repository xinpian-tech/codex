use std::collections::BTreeMap;
use std::fmt::Display;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use codex_infra_protocol::AgentId;
use codex_infra_protocol::InferenceBinding;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;
use futures::Stream;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;

mod recovery;
pub use recovery::ProviderAttemptIdentity;
pub use recovery::recover_provider_attempt;

const WIRE_LANES: [WireLane; 5] = [
    WireLane::Lifecycle,
    WireLane::ClientRequest,
    WireLane::ProviderRequest,
    WireLane::ProviderResponse,
    WireLane::ClientResponse,
];

/// Host-local spool and the Agent launch owning all attempts of this frontend.
#[derive(Clone)]
pub struct ProviderAuditConfig {
    pub directory: PathBuf,
    pub root_session_id: RootSessionId,
    pub machine_id: MachineId,
    pub agent_id: AgentId,
    pub launch_id: MessageId,
}

#[derive(Clone, Copy)]
pub(crate) enum WireLane {
    ClientRequest,
    ProviderRequest,
    ProviderResponse,
    ClientResponse,
    Lifecycle,
}

impl WireLane {
    fn name(self) -> &'static str {
        match self {
            Self::ClientRequest => "client-request",
            Self::ProviderRequest => "provider-request",
            Self::ProviderResponse => "provider-response",
            Self::ClientResponse => "client-response",
            Self::Lifecycle => "lifecycle",
        }
    }
}

#[derive(Clone)]
pub(crate) struct AttemptAudit {
    pub(crate) id: MessageId,
    journals: Arc<Mutex<AuditJournals>>,
}

struct AuditJournals {
    entries: Vec<(WireLane, Journal)>,
    completion: Journal,
    closed: bool,
    _run_lock: File,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderAttemptEnd {
    #[default]
    ResponseBodyFinished,
    OwnerExited,
}

/// Local producer closure after the client response body finishes consumption.
/// This records journal positions, not model success or remote delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderAttemptFinished {
    pub attempt_id: MessageId,
    #[serde(default)]
    pub end: ProviderAttemptEnd,
    pub positions: BTreeMap<String, JournalPosition>,
}

impl ProviderAttemptFinished {
    pub fn read(directory: &Path) -> io::Result<Option<Self>> {
        Ok(Self::read_with_position(directory)?.map(|(finished, _)| finished))
    }

    pub fn read_with_position(directory: &Path) -> io::Result<Option<(Self, JournalPosition)>> {
        let mut reader = match JournalReader::open(
            &directory.join("completion.journal"),
            JournalPosition::default(),
        ) {
            Ok(reader) => reader,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(record) = reader.next_record()? else {
            return Ok(None);
        };
        Ok(Some((
            serde_json::from_slice(&record.payload)?,
            reader.position(),
        )))
    }
}

impl AttemptAudit {
    pub(crate) async fn open(
        config: ProviderAuditConfig,
        binding: InferenceBinding,
    ) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let id = MessageId::new();
            let directory = config.directory.join(id.to_string());
            std::fs::create_dir_all(&directory)?;
            let run_lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(directory.join("run.lock"))?;
            run_lock.lock()?;
            let mut journals = Vec::new();
            for lane in WIRE_LANES {
                let mut journal =
                    Journal::open(&directory.join(format!("{}.journal", lane.name())), |_| {
                        Ok(())
                    })?;
                if matches!(lane, WireLane::Lifecycle) {
                    journal.append(&serde_json::to_vec(&json!({
                        "event": "opened", "attempt_id": id, "binding": binding,
                        "root_session_id": config.root_session_id, "machine_id": config.machine_id,
                        "agent_id": config.agent_id, "launch_id": config.launch_id,
                    }))?)?;
                }
                journals.push((lane, journal));
            }
            Ok(Self {
                id,
                journals: Arc::new(Mutex::new(AuditJournals {
                    entries: journals,
                    completion: Journal::open(&directory.join("completion.journal"), |_| Ok(()))?,
                    closed: false,
                    _run_lock: run_lock,
                })),
            })
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(crate) async fn record(&self, lane: WireLane, bytes: Bytes) -> io::Result<()> {
        let journals = Arc::clone(&self.journals);
        tokio::task::spawn_blocking(move || {
            let mut journals = journals
                .lock()
                .map_err(|_| io::Error::other("provider audit writer poisoned"))?;
            if journals.closed {
                return Err(io::Error::other("provider attempt journals closed"));
            }
            let (_, journal) = journals
                .entries
                .iter_mut()
                .find(|(candidate, _)| candidate.name() == lane.name())
                .ok_or_else(|| io::Error::other("provider audit lane missing"))?;
            journal.append(&bytes)?;
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(crate) async fn event(&self, event: Value) -> io::Result<()> {
        self.record(
            WireLane::Lifecycle,
            Bytes::from(serde_json::to_vec(&event)?),
        )
        .await
    }

    async fn finish(&self) -> io::Result<()> {
        let journals = Arc::clone(&self.journals);
        let attempt_id = self.id;
        tokio::task::spawn_blocking(move || {
            let mut journals = journals
                .lock()
                .map_err(|_| io::Error::other("provider audit writer poisoned"))?;
            if journals.closed {
                return Err(io::Error::other("provider attempt already closed"));
            }
            journals.closed = true;
            let finished = ProviderAttemptFinished {
                attempt_id,
                end: ProviderAttemptEnd::ResponseBodyFinished,
                positions: journals
                    .entries
                    .iter()
                    .map(|(lane, journal)| (lane.name().to_owned(), journal.position()))
                    .collect(),
            };
            journals
                .completion
                .append(&serde_json::to_vec(&finished)?)?;
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(crate) fn capture<S, E>(
        self,
        source: S,
        lane: WireLane,
    ) -> impl Stream<Item = io::Result<Bytes>> + Send
    where
        S: Stream<Item = Result<Bytes, E>> + Send,
        E: Display + Send,
    {
        async_stream::try_stream! {
            let mut source = Box::pin(source);
            while let Some(chunk) = source.next().await {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        self.event(json!({"event": "stream_failed", "stream": lane.name(), "error": error.to_string()})).await?;
                        Err(io::Error::other(error.to_string()))?
                    }
                };
                self.record(lane, chunk.clone()).await?;
                yield chunk;
            }
            drop(source);
            self.event(json!({"event": "stream_eof", "stream": lane.name()})).await?;
            if matches!(lane, WireLane::ClientResponse) {
                self.finish().await?;
            }
        }
    }
}
