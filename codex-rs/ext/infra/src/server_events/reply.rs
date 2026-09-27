use std::collections::BTreeMap;
use std::io;

use codex_app_server::in_process::InProcessClientSender;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::oneshot;

use super::AgentServerEvent;
use super::AgentServerEvents;

/// Enqueued confirms local app-server transport admission, not application of
/// the response. NotEnqueued permits another attempt while this run is alive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentServerReplyOutcome {
    Enqueued,
    NotEnqueued { error: String },
}

pub(super) struct LiveRequest {
    pub position: JournalPosition,
    pub id: RequestId,
    pub enqueued: Option<blake3::Hash>,
}

pub(super) struct ReplyCommand {
    position: JournalPosition,
    response: Result<Value, JSONRPCErrorError>,
    digest: blake3::Hash,
    done: oneshot::Sender<io::Result<AgentServerReplyOutcome>>,
}

impl AgentServerEvents {
    /// Replies only to a request captured by this runtime instance. The capture
    /// task owns the accepted command through both journal writes even if this
    /// waiter is cancelled. A repeated enqueued response reuses its result.
    pub async fn reply(
        &self,
        position: JournalPosition,
        response: Result<Value, JSONRPCErrorError>,
    ) -> io::Result<AgentServerReplyOutcome> {
        let digest = blake3::hash(&serde_json::to_vec(&response)?);
        let (done, result) = oneshot::channel();
        self.replies
            .send(ReplyCommand {
                position,
                response,
                digest,
                done,
            })
            .await
            .map_err(io::Error::other)?;
        result.await.map_err(io::Error::other)?
    }
}

pub(super) async fn handle(
    journal: Journal,
    requests: &mut BTreeMap<u64, LiveRequest>,
    sender: &InProcessClientSender,
    command: ReplyCommand,
) -> io::Result<Journal> {
    let Some(request) = requests
        .get_mut(&command.position.next_sequence)
        .filter(|request| request.position == command.position)
    else {
        let _ = command.done.send(Err(io::Error::other(
            "reply has no request in this runtime",
        )));
        return Ok(journal);
    };
    if let Some(previous) = request.enqueued {
        let result = if previous == command.digest {
            Ok(AgentServerReplyOutcome::Enqueued)
        } else {
            Err(io::Error::other("enqueued server reply content changed"))
        };
        let _ = command.done.send(result);
        return Ok(journal);
    }
    let request_id = request.id.clone();
    let sender = sender.clone();
    let position = command.position;
    let response = command.response;
    let (journal, outcome) = tokio::task::spawn_blocking(move || {
        let mut journal = journal;
        journal.append(&serde_json::to_vec(&AgentServerEvent::ReplyPrepared {
            request_position: position,
            response: response.clone(),
        })?)?;
        let sent = match response {
            Ok(value) => sender.respond_to_server_request(request_id, value),
            Err(error) => sender.fail_server_request(request_id, error),
        };
        let outcome = match sent {
            Ok(()) => AgentServerReplyOutcome::Enqueued,
            Err(error) => AgentServerReplyOutcome::NotEnqueued {
                error: error.to_string(),
            },
        };
        journal.append(&serde_json::to_vec(&AgentServerEvent::ReplySubmitted {
            request_position: position,
            outcome: outcome.clone(),
        })?)?;
        Ok::<_, io::Error>((journal, outcome))
    })
    .await
    .map_err(io::Error::other)??;
    if outcome == AgentServerReplyOutcome::Enqueued {
        request.enqueued = Some(command.digest);
    }
    let _ = command.done.send(Ok(outcome));
    Ok(journal)
}
