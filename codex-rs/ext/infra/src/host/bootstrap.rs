use std::io;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;

use codex_app_server_protocol::RequestId;
use codex_infra_protocol::MessageKind;
use codex_infra_protocol::TaskSpec;
use codex_infra_runtime::TerminalMailbox;
use codex_infra_state::InboxEntry;
use codex_infra_state::JournalPosition;
use codex_infra_state::PresentedInput;

use super::StartedAgentHost;
use crate::AgentInputDelivery;
use crate::AgentInputDeliveryProgress;
use crate::AgentInputEvidence;
use crate::AgentInputEvidenceStatus;
use crate::AgentInputTurnOutcome;
use crate::AgentMessageInput;
use crate::AgentRpcOutcome;
use crate::AgentThreadOutcome;

/// Startup progress. Presentation and turn association are separate from Task
/// completion. Recovery outcomes remain available to the owning scheduler.
pub enum AgentBootstrapOutcome {
    ThreadUncertain {
        request_id: RequestId,
    },
    Presented {
        input: PresentedInput,
    },
    InjectionUnresolved {
        outcome: AgentRpcOutcome,
    },
    StoreRecoveryRequired {
        status: AgentInputEvidenceStatus,
    },
    Turn {
        outcome: AgentInputTurnOutcome,
        delivery: Box<AgentInputDelivery>,
    },
}

impl StartedAgentHost {
    /// Drives initial input while the owner concurrently consumes server events.
    /// Call during exclusive startup, before other input/turn/history dispatch.
    /// Drive this future to its outcome before beginning shutdown.
    pub async fn begin_bootstrap(
        &self,
        entry: &InboxEntry,
        terminal: &TerminalMailbox,
        thread_page_size: NonZeroU32,
        audit_page_size: NonZeroUsize,
        evidence_capacity: NonZeroUsize,
    ) -> io::Result<AgentBootstrapOutcome> {
        let message = &entry.message;
        if message.kind != MessageKind::Bootstrap || message.task_id != self.task.task_id {
            return Err(io::Error::other("bootstrap differs from prepared Task"));
        }
        let task: TaskSpec = serde_json::from_str(&message.body)?;
        if serde_json::to_value(task)? != serde_json::to_value(&self.task)? {
            return Err(io::Error::other("bootstrap Task content changed"));
        }
        let input = AgentMessageInput::from_inbox(entry, &self.launch)?;
        let client = self.host.request_client();
        let thread_id = match self.thread.ensure(&client, thread_page_size).await? {
            AgentThreadOutcome::Ready { thread_id } => thread_id,
            AgentThreadOutcome::Uncertain { request_id } => {
                return Ok(AgentBootstrapOutcome::ThreadUncertain { request_id });
            }
        };
        let mut delivery = self
            .inputs
            .track_delivery(entry, thread_id.clone(), evidence_capacity)
            .await?;
        loop {
            match delivery.advance(&client, terminal, audit_page_size).await? {
                AgentInputDeliveryProgress::Scanning => {}
                AgentInputDeliveryProgress::Waiting => break,
                AgentInputDeliveryProgress::Confirmed { input } => {
                    return Ok(AgentBootstrapOutcome::Presented { input });
                }
            }
        }
        // Only append operations begun after this sampled prefix count here.
        // Historical flush evidence does not establish current thread contents.
        let baseline = self
            .host
            .read_store_audit(JournalPosition::default(), audit_page_size)
            .await?
            .durable_end;
        let mut evidence = AgentInputEvidence::since(&input, thread_id.clone(), baseline)?;
        let (submission, outcome) = self.inputs.inject(&client, entry, thread_id).await?;
        match outcome {
            AgentRpcOutcome::Reply(Ok(_)) => {}
            outcome @ (AgentRpcOutcome::Reply(Err(_)) | AgentRpcOutcome::Uncertain { .. }) => {
                return Ok(AgentBootstrapOutcome::InjectionUnresolved { outcome });
            }
        }
        loop {
            let page = self
                .host
                .read_store_audit(evidence.cursor(), audit_page_size)
                .await?;
            let status = evidence.observe(&page)?;
            if matches!(status, AgentInputEvidenceStatus::Flushed { .. }) {
                let outcome = self.inputs.start_turn(&client, &submission).await?;
                return Ok(AgentBootstrapOutcome::Turn {
                    outcome,
                    delivery: Box::new(delivery),
                });
            }
            if page.next == page.durable_end {
                return Ok(AgentBootstrapOutcome::StoreRecoveryRequired { status });
            }
        }
    }
}
