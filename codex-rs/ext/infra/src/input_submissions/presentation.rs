use std::io;
use std::sync::Arc;

use codex_infra_state::PresentedInput;

use super::AgentInputSubmissions;
use super::Event;
use crate::AgentInputPresentation;

impl AgentInputSubmissions {
    /// Persists evidence from AgentInputPresentationScan before the terminal
    /// mailbox emits its Presented receipt. The consumed turn may differ from
    /// turn/start's response when an active turn finishes during injection.
    /// This neither marks the Task processed nor sends any message itself.
    pub async fn record_presentation(
        &self,
        evidence: &AgentInputPresentation,
    ) -> io::Result<PresentedInput> {
        if evidence.turn_id.is_empty()
            || evidence.response_id.is_empty()
            || evidence.completed_sequence <= evidence.prepared_sequence
        {
            return Err(io::Error::other(
                "input presentation evidence is incomplete",
            ));
        }
        let evidence = evidence.clone();
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let mut writer = writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            if writer.closed {
                return Err(io::Error::other("input submission admission is closed"));
            }
            let Some((submission, _)) = writer.entries.get(&evidence.message_id) else {
                return Err(io::Error::other("presentation has no prepared input"));
            };
            if submission.thread_id != evidence.thread_id {
                return Err(io::Error::other("input presentation thread changed"));
            }
            if let Some(previous) = writer.presentations.get(&evidence.message_id) {
                if previous != &evidence {
                    return Err(io::Error::other("input presentation evidence changed"));
                }
            } else {
                writer
                    .journal
                    .append(&serde_json::to_vec(&Event::Presented {
                        evidence: evidence.clone(),
                    })?)?;
                writer
                    .presentations
                    .insert(evidence.message_id, evidence.clone());
            }
            Ok(PresentedInput {
                thread_id: evidence.thread_id,
                turn_id: evidence.turn_id,
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}
