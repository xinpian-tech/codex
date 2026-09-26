use std::fmt::Display;
use std::io;
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
use futures::Stream;
use futures::StreamExt;
use serde_json::Value;
use serde_json::json;

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
    journals: Arc<Mutex<Vec<(WireLane, Journal)>>>,
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
            let mut journals = Vec::new();
            for lane in [
                WireLane::ClientRequest,
                WireLane::ProviderRequest,
                WireLane::ProviderResponse,
                WireLane::ClientResponse,
                WireLane::Lifecycle,
            ] {
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
                journals: Arc::new(Mutex::new(journals)),
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
            let (_, journal) = journals
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
            futures::pin_mut!(source);
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
            self.event(json!({"event": "stream_eof", "stream": lane.name()})).await?;
        }
    }
}
