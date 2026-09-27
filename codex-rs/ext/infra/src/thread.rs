use std::io;
use std::num::NonZeroU32;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_app_server::in_process::InProcessClientSender;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadSource;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_infra_protocol::MessageId;
use codex_infra_runtime::LaunchIntent;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;

use crate::AgentRpc;
use crate::AgentRpcOutcome;
use crate::ManagedHost;

/// A missing response remains unresolved until a matching durable thread is found.
pub enum AgentThreadOutcome {
    Ready { thread_id: String },
    Uncertain { request_id: RequestId },
}

/// Persists a launch's create intent and selected thread independently of RPC
/// transport completion. Callers drive this before presenting inbox messages.
#[derive(Clone)]
pub struct AgentThread {
    launch: LaunchIntent,
    writer: Arc<Mutex<Writer>>,
    gate: Arc<tokio::sync::Mutex<bool>>,
}

struct Writer {
    path: PathBuf,
    journal: Journal,
    create_id: RequestId,
    thread_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event {
    Opened {
        launch: Box<LaunchIntent>,
        create_id: RequestId,
    },
    Bound {
        thread_id: String,
        evidence_request: RequestId,
    },
}

impl AgentThread {
    /// Call on a blocking worker, before issuing any thread/start request.
    pub fn open(path: &Path, launch: LaunchIntent) -> io::Result<Self> {
        let mut create_id = None;
        let mut thread_id = None;
        let mut journal = Journal::open(path, |record| {
            match serde_json::from_slice::<Event>(&record.payload)? {
                Event::Opened {
                    launch: previous,
                    create_id: id,
                } => {
                    if *previous != launch
                        || create_id.as_ref().is_some_and(|previous| previous != &id)
                    {
                        return Err(io::Error::other("Agent thread launch binding changed"));
                    }
                    create_id = Some(id);
                }
                Event::Bound { thread_id: id, .. } => {
                    if create_id.is_none()
                        || thread_id.as_ref().is_some_and(|previous| previous != &id)
                    {
                        return Err(io::Error::other("Agent thread selection changed"));
                    }
                    thread_id = Some(id);
                }
            }
            Ok(())
        })?;
        let create_id = match create_id {
            Some(id) => id,
            None => {
                let id = RequestId::String(MessageId::new().to_string());
                journal.append(&serde_json::to_vec(&Event::Opened {
                    launch: Box::new(launch.clone()),
                    create_id: id.clone(),
                })?)?;
                id
            }
        };
        Ok(Self {
            launch,
            writer: Arc::new(Mutex::new(Writer {
                path: path.canonicalize()?,
                journal,
                create_id,
                thread_id,
            })),
            gate: Arc::default(),
        })
    }

    /// Replays a recorded creation response or searches paginated metadata after
    /// an uncertain create. A fresh resume request confirms the thread is loaded
    /// in this app-server; replaying an old resume reply would not establish that.
    pub async fn ensure(
        &self,
        host: &ManagedHost,
        page_size: NonZeroU32,
    ) -> io::Result<AgentThreadOutcome> {
        let owner = self.clone();
        let rpc = host.rpc.clone();
        let sender = host.client.sender();
        tokio::spawn(async move {
            let gate = Arc::clone(&owner.gate).lock_owned().await;
            if *gate {
                return Err(io::Error::other("Agent thread owner is closed"));
            }
            owner.ensure_inner(rpc, sender, page_size).await
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Finishes owned binding operations and returns the journal's final prefix.
    /// The host separately drains RPC and inference before final publication.
    pub async fn close(&self) -> io::Result<(PathBuf, JournalPosition)> {
        let owner = self.clone();
        tokio::spawn(async move {
            *owner.gate.lock().await = true;
            tokio::task::spawn_blocking(move || {
                let writer = owner
                    .writer
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                Ok((writer.path.clone(), writer.journal.position()))
            })
            .await
            .map_err(io::Error::other)?
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn ensure_inner(
        &self,
        rpc: AgentRpc,
        sender: InProcessClientSender,
        page_size: NonZeroU32,
    ) -> io::Result<AgentThreadOutcome> {
        let (create_id, bound) = {
            let writer = self
                .writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            (writer.create_id.clone(), writer.thread_id.clone())
        };
        let thread_id = match bound {
            Some(id) => id,
            None => {
                let result = rpc
                    .request(
                        sender.clone(),
                        ClientRequest::ThreadStart {
                            request_id: create_id.clone(),
                            params: ThreadStartParams {
                                model: Some(self.launch.generation.inference.model_id.clone()),
                                model_provider: Some(
                                    self.launch.generation.inference.provider_id.clone(),
                                ),
                                cwd: Some(
                                    self.launch
                                        .workspace
                                        .worktree
                                        .to_str()
                                        .ok_or_else(|| io::Error::other("Agent cwd is not UTF-8"))?
                                        .to_owned(),
                                ),
                                ephemeral: Some(false),
                                history_mode: Some(ThreadHistoryMode::Paginated),
                                thread_source: Some(ThreadSource::Feature(format!(
                                    "infra:{}",
                                    self.launch.launch_id
                                ))),
                                ..Default::default()
                            },
                        },
                    )
                    .await?;
                let (thread, evidence) = match result {
                    AgentRpcOutcome::Reply(Ok(value)) => {
                        let response: ThreadStartResponse = serde_json::from_value(value)?;
                        if response.model != self.launch.generation.inference.model_id {
                            return Err(io::Error::other("created thread uses another model"));
                        }
                        (response.thread, create_id.clone())
                    }
                    AgentRpcOutcome::Reply(Err(error)) => {
                        return Err(io::Error::other(serde_json::to_string(&error)?));
                    }
                    AgentRpcOutcome::Uncertain { .. } => {
                        let Some(found) = self.discover(&rpc, &sender, page_size).await? else {
                            return Ok(AgentThreadOutcome::Uncertain {
                                request_id: create_id,
                            });
                        };
                        found
                    }
                };
                self.validate(&thread)?;
                let id = thread.id.clone();
                let selected = id.clone();
                let writer = Arc::clone(&self.writer);
                tokio::task::spawn_blocking(move || {
                    let mut writer = writer
                        .lock()
                        .map_err(|error| io::Error::other(error.to_string()))?;
                    writer.journal.append(&serde_json::to_vec(&Event::Bound {
                        thread_id: selected.clone(),
                        evidence_request: evidence,
                    })?)?;
                    writer.thread_id = Some(selected);
                    Ok::<_, io::Error>(())
                })
                .await
                .map_err(io::Error::other)??;
                id
            }
        };
        let request_id = RequestId::String(MessageId::new().to_string());
        match rpc
            .request(
                sender,
                ClientRequest::ThreadResume {
                    request_id: request_id.clone(),
                    params: ThreadResumeParams {
                        thread_id: thread_id.clone(),
                        ..Default::default()
                    },
                },
            )
            .await?
        {
            AgentRpcOutcome::Reply(Ok(value)) => {
                let response: ThreadResumeResponse = serde_json::from_value(value)?;
                if response.model != self.launch.generation.inference.model_id {
                    return Err(io::Error::other("resumed thread uses another model"));
                }
                self.validate(&response.thread)?;
                if response.thread.id != thread_id {
                    return Err(io::Error::other("resumed another Agent thread"));
                }
                Ok(AgentThreadOutcome::Ready { thread_id })
            }
            AgentRpcOutcome::Reply(Err(error)) => {
                Err(io::Error::other(serde_json::to_string(&error)?))
            }
            AgentRpcOutcome::Uncertain { .. } => Ok(AgentThreadOutcome::Uncertain { request_id }),
        }
    }

    async fn discover(
        &self,
        rpc: &AgentRpc,
        sender: &InProcessClientSender,
        page_size: NonZeroU32,
    ) -> io::Result<Option<(Thread, RequestId)>> {
        let marker = ThreadSource::Feature(format!("infra:{}", self.launch.launch_id));
        let mut found = None;
        for archived in [false, true] {
            let mut cursor = None::<String>;
            loop {
                let request_id = RequestId::String(MessageId::new().to_string());
                let request = ClientRequest::ThreadList {
                    request_id: request_id.clone(),
                    params: serde_json::from_value(json!({
                        "cursor": cursor, "limit": page_size.get(), "archived": archived,
                        "sourceKinds": ["exec"], "modelProviders": [self.launch.generation.inference.provider_id],
                    }))?,
                };
                let page: ThreadListResponse = match rpc.request(sender.clone(), request).await? {
                    AgentRpcOutcome::Reply(Ok(value)) => serde_json::from_value(value)?,
                    AgentRpcOutcome::Reply(Err(error)) => {
                        return Err(io::Error::other(serde_json::to_string(&error)?));
                    }
                    AgentRpcOutcome::Uncertain { .. } => return Ok(None),
                };
                for thread in page.data {
                    if thread.thread_source.as_ref() == Some(&marker) {
                        self.validate(&thread)?;
                        if found
                            .as_ref()
                            .is_some_and(|(previous, _): &(Thread, RequestId)| {
                                previous.id != thread.id
                            })
                        {
                            return Err(io::Error::other("multiple threads match Agent launch"));
                        }
                        found = Some((thread, request_id.clone()));
                    }
                }
                match page.next_cursor {
                    Some(next) if Some(&next) != cursor.as_ref() => cursor = Some(next),
                    Some(_) => {
                        return Err(io::Error::other("thread discovery cursor did not advance"));
                    }
                    None => break,
                }
            }
        }
        Ok(found)
    }

    fn validate(&self, thread: &Thread) -> io::Result<()> {
        if thread.thread_source
            != Some(ThreadSource::Feature(format!(
                "infra:{}",
                self.launch.launch_id
            )))
            || thread.cwd.as_path() != self.launch.workspace.worktree
            || thread.model_provider != self.launch.generation.inference.provider_id
        {
            return Err(io::Error::other(
                "thread metadata differs from Agent launch",
            ));
        }
        Ok(())
    }
}
