use std::io;
use std::sync::Arc;

use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::TurnStartResponse;

use super::AgentInputSubmission;
use super::AgentInputSubmissions;
use super::Event;
use crate::AgentRpcOutcome;
use crate::ManagedHost;

/// A recorded association is historical, not a claim that the turn is still
/// running or that it has consumed the injected fragments.
pub enum AgentInputTurnOutcome {
    Associated {
        thread_id: String,
        turn_id: String,
    },
    Rejected {
        error: JSONRPCErrorError,
    },
    Uncertain {
        request_id: RequestId,
        error: Option<String>,
    },
}

impl AgentInputSubmissions {
    /// Dispatches the stable turn intent and persists its returned association.
    /// The driver coordinates injection and active-turn state first. Empty
    /// turn/start can steer an existing turn, so the returned ID alone cannot
    /// establish which turn eventually consumes an in-flight injected item.
    /// Cancellation drops only the waiter; close waits for the owned dispatch.
    pub async fn start_turn(
        &self,
        host: &ManagedHost,
        submission: &AgentInputSubmission,
    ) -> io::Result<AgentInputTurnOutcome> {
        let inputs = self.clone();
        let submission = submission.clone();
        let rpc = host.rpc.clone();
        let sender = host.client.sender();
        tokio::spawn(async move {
            let _dispatch = Arc::clone(&inputs.dispatch).lock_owned().await;
            let request = inputs.prepare_turn(&submission).await?;
            let request_id = request.id().clone();
            let outcome = rpc.request(sender, request).await?;
            let turn_id = match outcome {
                AgentRpcOutcome::Reply(Ok(value)) => {
                    let response: TurnStartResponse = serde_json::from_value(value)?;
                    if response.turn.id.is_empty() {
                        return Err(io::Error::other("turn/start returned an empty turn ID"));
                    }
                    response.turn.id
                }
                AgentRpcOutcome::Reply(Err(error)) => {
                    return Ok(AgentInputTurnOutcome::Rejected { error });
                }
                AgentRpcOutcome::Uncertain { request_id, error } => {
                    return Ok(AgentInputTurnOutcome::Uncertain { request_id, error });
                }
            };
            tokio::task::spawn_blocking(move || {
                let mut writer = inputs
                    .writer
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                let prepared_id = *writer
                    .turns
                    .get(&submission.message_id)
                    .ok_or_else(|| io::Error::other("turn binding has no prepared request"))?;
                if request_id != RequestId::String(prepared_id.to_string()) {
                    return Err(io::Error::other("turn binding request changed"));
                }
                if let Some(previous) = writer.bound_turns.get(&submission.message_id) {
                    if previous != &turn_id {
                        return Err(io::Error::other("input turn association changed"));
                    }
                } else {
                    writer
                        .journal
                        .append(&serde_json::to_vec(&Event::TurnBound {
                            message_id: submission.message_id,
                            request_id: prepared_id,
                            turn_id: turn_id.clone(),
                        })?)?;
                    writer
                        .bound_turns
                        .insert(submission.message_id, turn_id.clone());
                }
                Ok(AgentInputTurnOutcome::Associated {
                    thread_id: submission.thread_id,
                    turn_id,
                })
            })
            .await
            .map_err(io::Error::other)?
        })
        .await
        .map_err(io::Error::other)?
    }
}
