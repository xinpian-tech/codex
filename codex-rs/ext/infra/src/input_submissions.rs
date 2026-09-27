use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::TurnStartParams;
use codex_infra_protocol::MessageId;
use codex_infra_runtime::LaunchIntent;
use codex_infra_state::InboxEntry;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::AgentInputPresentation;
use crate::AgentMessageInput;

mod inject;
mod presentation;
mod turn;
pub use turn::AgentInputTurnOutcome;

/// Durable injection intent. Its request ID is reused after interruption so the
/// RPC ledger can distinguish an unsent request from an uncertain submission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInputSubmission {
    pub message_id: MessageId,
    pub request_id: MessageId,
    pub thread_id: String,
    pub accepted_sequence: u64,
}

/// Owns per-launch input intents independently of the inbox and RPC journals.
#[derive(Clone)]
pub struct AgentInputSubmissions {
    launch: LaunchIntent,
    writer: Arc<Mutex<Writer>>,
    dispatch: Arc<tokio::sync::Mutex<()>>,
}

struct Writer {
    path: PathBuf,
    journal: Journal,
    entries: BTreeMap<MessageId, (AgentInputSubmission, blake3::Hash)>,
    turns: BTreeMap<MessageId, MessageId>,
    bound_turns: BTreeMap<MessageId, String>,
    presentations: BTreeMap<MessageId, AgentInputPresentation>,
    closed: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event {
    Opened {
        launch: Box<LaunchIntent>,
    },
    Prepared {
        submission: AgentInputSubmission,
        items: Vec<Value>,
    },
    TurnPrepared {
        message_id: MessageId,
        request_id: MessageId,
    },
    TurnBound {
        message_id: MessageId,
        request_id: MessageId,
        turn_id: String,
    },
    Presented {
        evidence: AgentInputPresentation,
    },
}

impl AgentInputSubmissions {
    /// Open on a blocking worker before input dispatch. A launch must retain
    /// the same inbox and thread binding when reopening this journal.
    pub fn open(path: &Path, launch: LaunchIntent) -> io::Result<Self> {
        let mut opened = false;
        let mut entries = BTreeMap::new();
        let mut turns = BTreeMap::new();
        let mut bound_turns = BTreeMap::new();
        let mut presentations = BTreeMap::new();
        let mut journal = Journal::open(path, |record| {
            match serde_json::from_slice::<Event>(&record.payload)? {
                Event::Opened { launch: previous } => {
                    if *previous != launch {
                        return Err(io::Error::other("input submission launch changed"));
                    }
                    opened = true;
                }
                Event::Prepared { submission, items } => {
                    if !opened || entries.contains_key(&submission.message_id) {
                        return Err(io::Error::other("invalid input submission journal order"));
                    }
                    let digest = blake3::hash(&serde_json::to_vec(&items)?);
                    entries.insert(submission.message_id, (submission, digest));
                }
                Event::TurnPrepared {
                    message_id,
                    request_id,
                } => {
                    if !entries.contains_key(&message_id)
                        || turns.insert(message_id, request_id).is_some()
                    {
                        return Err(io::Error::other("invalid input turn intent order"));
                    }
                }
                Event::TurnBound {
                    message_id,
                    request_id,
                    turn_id,
                } => {
                    if turns.get(&message_id) != Some(&request_id)
                        || turn_id.is_empty()
                        || bound_turns.insert(message_id, turn_id).is_some()
                    {
                        return Err(io::Error::other("invalid input turn binding"));
                    }
                }
                Event::Presented { evidence } => {
                    let Some((submission, _)) = entries.get(&evidence.message_id) else {
                        return Err(io::Error::other("presentation has no prepared input"));
                    };
                    if submission.thread_id != evidence.thread_id
                        || presentations
                            .insert(evidence.message_id, evidence)
                            .is_some()
                    {
                        return Err(io::Error::other("input presentation binding changed"));
                    }
                }
            }
            Ok(())
        })?;
        if !opened {
            journal.append(&serde_json::to_vec(&Event::Opened {
                launch: Box::new(launch.clone()),
            })?)?;
        }
        Ok(Self {
            launch,
            writer: Arc::new(Mutex::new(Writer {
                path: path.canonicalize()?,
                journal,
                entries,
                turns,
                bound_turns,
                presentations,
                closed: false,
            })),
            dispatch: Arc::default(),
        })
    }

    /// Records exact typed input before the first RPC attempt. Repeated calls
    /// for the same inbox entry return the original request ID; changed content
    /// or a different thread does not silently create another injection.
    pub async fn prepare(
        &self,
        entry: &InboxEntry,
        thread_id: String,
    ) -> io::Result<AgentInputSubmission> {
        if thread_id.is_empty() {
            return Err(io::Error::other("input submission requires a thread ID"));
        }
        let input = AgentMessageInput::from_inbox(entry, &self.launch)?;
        let items = input.response_items()?;
        let digest = blake3::hash(&serde_json::to_vec(&items)?);
        let proposed = AgentInputSubmission {
            message_id: input.message_id(),
            request_id: MessageId::new(),
            thread_id,
            accepted_sequence: entry.accepted_sequence,
        };
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let mut writer = writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            if writer.closed {
                return Err(io::Error::other("input submission admission is closed"));
            }
            if let Some((previous, previous_digest)) = writer.entries.get(&proposed.message_id) {
                if previous.thread_id != proposed.thread_id
                    || previous.accepted_sequence != proposed.accepted_sequence
                    || previous_digest != &digest
                {
                    return Err(io::Error::other("input submission binding changed"));
                }
                return Ok(previous.clone());
            }
            writer
                .journal
                .append(&serde_json::to_vec(&Event::Prepared {
                    submission: proposed.clone(),
                    items,
                })?)?;
            writer
                .entries
                .insert(proposed.message_id, (proposed.clone(), digest));
            Ok(proposed)
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Prepares an empty-input turn/start after the caller has reconciled the
    /// injection. Peer text remains in typed context fragments, not user.text.
    /// Submit the returned request through AgentRpc; uncertain results require
    /// reconciliation, and a turn/start reply alone is not a Presented receipt.
    /// Reopening preserves the request ID and does not start a second turn.
    pub async fn prepare_turn(
        &self,
        submission: &AgentInputSubmission,
    ) -> io::Result<ClientRequest> {
        let params = TurnStartParams {
            thread_id: submission.thread_id.clone(),
            input: Vec::new(),
            turn_trigger: Some(format!("infra:{}", submission.message_id)),
            ..Default::default()
        };
        let submission = submission.clone();
        let writer = Arc::clone(&self.writer);
        let request_id = tokio::task::spawn_blocking(move || {
            let mut writer = writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            if writer.closed {
                return Err(io::Error::other("input submission admission is closed"));
            }
            let Some((recorded, _)) = writer.entries.get(&submission.message_id) else {
                return Err(io::Error::other("turn input was not prepared"));
            };
            if recorded != &submission {
                return Err(io::Error::other("turn input binding changed"));
            }
            if let Some(request_id) = writer.turns.get(&submission.message_id) {
                return Ok(*request_id);
            }
            let request_id = MessageId::new();
            writer
                .journal
                .append(&serde_json::to_vec(&Event::TurnPrepared {
                    message_id: submission.message_id,
                    request_id,
                })?)?;
            writer.turns.insert(submission.message_id, request_id);
            Ok(request_id)
        })
        .await
        .map_err(io::Error::other)??;
        Ok(ClientRequest::TurnStart {
            request_id: RequestId::String(request_id.to_string()),
            params,
        })
    }

    /// Waits for owned input/turn dispatch, closes intent admission, and returns its
    /// final archive boundary. Call before closing the host RPC ledger. The
    /// driver must separately settle inference and the RPC ledger before
    /// acknowledging presentation or completing host shutdown.
    pub async fn close(&self) -> io::Result<(PathBuf, JournalPosition)> {
        let writer = Arc::clone(&self.writer);
        let dispatch = Arc::clone(&self.dispatch);
        tokio::spawn(async move {
            let _dispatch = dispatch.lock_owned().await;
            tokio::task::spawn_blocking(move || {
                let mut writer = writer
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                writer.closed = true;
                Ok((writer.path.clone(), writer.journal.position()))
            })
            .await
            .map_err(io::Error::other)?
        })
        .await
        .map_err(io::Error::other)?
    }
}
