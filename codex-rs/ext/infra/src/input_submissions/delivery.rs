use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;

use codex_infra_runtime::TerminalMailbox;
use codex_infra_state::InboxEntry;
use codex_infra_state::PresentedInput;

use super::AgentInputSubmission;
use super::AgentInputSubmissions;
use crate::AgentHostClient;
use crate::AgentInputPresentation;
use crate::AgentInputPresentationScan;
use crate::AgentMessageInput;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentInputDeliveryProgress {
    /// More durable evidence is available; schedule another bounded step.
    Scanning,
    /// Caught up with the sampled prefix; wait for new model evidence.
    Waiting,
    /// The evidence and inbox binding are durable and the receipt was flushed.
    Confirmed { input: PresentedInput },
}

/// Drives one message's presentation without injecting input or starting turns.
/// The terminal loop owns this state between pages. After a restart it either
/// uses recorded presentation evidence or reconstructs it from the audit.
pub struct AgentInputDelivery {
    inputs: AgentInputSubmissions,
    submission: AgentInputSubmission,
    scan: AgentInputPresentationScan,
    evidence: Option<AgentInputPresentation>,
    confirmed: Option<PresentedInput>,
}

impl AgentInputSubmissions {
    /// Validates the inbox payload against the original intent, then restores
    /// any completed presentation. The same intent is shared with inject.
    pub async fn track_delivery(
        &self,
        entry: &InboxEntry,
        thread_id: String,
        evidence_capacity: NonZeroUsize,
    ) -> io::Result<AgentInputDelivery> {
        let submission = self.prepare(entry, thread_id).await?;
        let input = AgentMessageInput::from_inbox(entry, &self.launch)?;
        let scan = AgentInputPresentationScan::new(
            &input,
            submission.thread_id.clone(),
            evidence_capacity,
        )?;
        let writer = Arc::clone(&self.writer);
        let message_id = submission.message_id;
        let evidence = tokio::task::spawn_blocking(move || {
            let writer = writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            Ok::<_, io::Error>(writer.presentations.get(&message_id).cloned())
        })
        .await
        .map_err(io::Error::other)??;
        Ok(AgentInputDelivery {
            inputs: self.clone(),
            submission,
            scan,
            evidence,
            confirmed: None,
        })
    }
}

impl AgentInputDelivery {
    /// Reads at most one audit page, then records proof before emitting a
    /// Presented receipt through tmux stdout. Cancellation can leave either
    /// durable write complete; retry reuses those bindings and receipt sequence.
    /// This never sends Processed or treats model completion as Task completion.
    pub async fn advance(
        &mut self,
        host: &AgentHostClient,
        terminal: &TerminalMailbox,
        page_size: NonZeroUsize,
    ) -> io::Result<AgentInputDeliveryProgress> {
        if let Some(input) = &self.confirmed {
            return Ok(AgentInputDeliveryProgress::Confirmed {
                input: input.clone(),
            });
        }
        if self.evidence.is_none() {
            let page = host
                .model_inputs
                .read_page(self.scan.cursor(), page_size)
                .await?;
            self.scan.observe(&page)?;
            self.evidence = self.scan.presentation().cloned();
            if self.evidence.is_none() {
                return Ok(if page.next == page.durable_end {
                    AgentInputDeliveryProgress::Waiting
                } else {
                    AgentInputDeliveryProgress::Scanning
                });
            }
        }
        let evidence = self
            .evidence
            .as_ref()
            .ok_or_else(|| io::Error::other("presentation evidence missing"))?;
        let input = self.inputs.record_presentation(evidence).await?;
        let message_id = self.submission.message_id;
        let presented = input.clone();
        terminal
            .with_mailbox(move |mailbox| mailbox.confirm_presented(message_id, presented))
            .await?;
        self.confirmed = Some(input.clone());
        Ok(AgentInputDeliveryProgress::Confirmed { input })
    }
}
