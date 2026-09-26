use std::collections::BTreeMap;
use std::io;

use codex_extension_api::ToolExecutionKind;
use codex_extension_api::ToolExecutionOrigin;
use codex_infra_protocol::MessageId;
use serde::Deserialize;
use serde::Serialize;

use crate::StoreAuditIdentity;
use crate::ToolAuditEvent;
use crate::ToolOperation;
use crate::ToolOutcome;

#[derive(Debug, Serialize, Deserialize)]
struct PendingTool {
    #[serde(default)]
    origin: Option<ToolExecutionOrigin>,
    #[serde(default)]
    execution_kind: Option<ToolExecutionKind>,
    #[serde(default)]
    admitted_sequence: Option<u64>,
    started_sequence: Option<u64>,
    operation: ToolOperation,
}

/// Handler completion to reconcile with the processes attributed to this call.
#[derive(Debug, Serialize, Deserialize)]
pub struct ToolSettlement {
    #[serde(default)]
    pub origin: Option<ToolExecutionOrigin>,
    #[serde(default)]
    pub execution_kind: Option<ToolExecutionKind>,
    #[serde(default)]
    pub admitted_sequence: Option<u64>,
    pub started_sequence: Option<u64>,
    pub finished_sequence: u64,
    pub operation: ToolOperation,
    pub outcome: ToolOutcome,
}

/// Replayable handler activity for one launch, excluding historical payloads.
/// Consumers persist their cursor and this view after handling each settlement.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ToolActivity {
    next_sequence: u64,
    binding: Option<(StoreAuditIdentity, MessageId)>,
    pending: BTreeMap<String, PendingTool>,
    failure: Option<String>,
}

impl ToolActivity {
    pub fn apply(
        &mut self,
        sequence: u64,
        event: &ToolAuditEvent,
    ) -> io::Result<Option<ToolSettlement>> {
        if let Some(error) = &self.failure {
            return Err(io::Error::other(error.clone()));
        }
        let result = self.apply_next(sequence, event);
        if let Err(error) = &result {
            self.failure = Some(error.to_string());
        }
        result
    }

    /// Requires quiescent dispatch and a consumer caught up to its watermark.
    /// Handler completion does not establish that child writers have stopped.
    pub fn require_settled(&self) -> io::Result<()> {
        if let Some(error) = &self.failure {
            return Err(io::Error::other(error.clone()));
        }
        if self.binding.is_none() || !self.pending.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "tool activity is unresolved",
            ));
        }
        Ok(())
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    fn apply_next(
        &mut self,
        sequence: u64,
        event: &ToolAuditEvent,
    ) -> io::Result<Option<ToolSettlement>> {
        if sequence != self.next_sequence {
            return Err(io::Error::other("tool audit sequence discontinuity"));
        }
        if self.binding.is_none() && !matches!(event, ToolAuditEvent::Opened { .. }) {
            return Err(io::Error::other("tool audit opening record missing"));
        }
        let settlement = match event {
            ToolAuditEvent::Opened {
                identity,
                launch_id,
            } => {
                let binding = (identity.clone(), *launch_id);
                if self
                    .binding
                    .as_ref()
                    .is_some_and(|previous| previous != &binding)
                {
                    return Err(io::Error::other("tool audit binding changed"));
                }
                self.binding = Some(binding);
                None
            }
            ToolAuditEvent::Admitted {
                operation,
                execution_kind,
                origin,
            } => {
                let key = operation_key(operation)?;
                if self.pending.contains_key(&key) {
                    return Err(io::Error::other("tool operation already admitted"));
                }
                self.pending.insert(
                    key,
                    PendingTool {
                        origin: origin.clone(),
                        execution_kind: *execution_kind,
                        admitted_sequence: Some(sequence),
                        started_sequence: None,
                        operation: operation.clone(),
                    },
                );
                None
            }
            ToolAuditEvent::Started { operation, .. } => {
                let pending = self
                    .pending
                    .entry(operation_key(operation)?)
                    .or_insert_with(|| PendingTool {
                        origin: None,
                        execution_kind: None,
                        admitted_sequence: None,
                        started_sequence: None,
                        operation: operation.clone(),
                    });
                if pending.started_sequence.is_some() || &pending.operation != operation {
                    return Err(io::Error::other("tool start identity or phase mismatch"));
                }
                pending.started_sequence = Some(sequence);
                None
            }
            ToolAuditEvent::Finished { operation, outcome } => {
                let pending = self.pending.remove(&operation_key(operation)?);
                let (admitted_sequence, started_sequence, execution_kind, origin) = match pending {
                    Some(pending) => {
                        if &pending.operation != operation {
                            return Err(io::Error::other("tool finish identity mismatch"));
                        }
                        (
                            pending.admitted_sequence,
                            pending.started_sequence,
                            pending.execution_kind,
                            pending.origin,
                        )
                    }
                    // MCP preparation can fail inside its handler before the
                    // start callback. Preserve missing provenance rather than
                    // infer an external invocation from the handler outcome.
                    None => (None, None, None, None),
                };
                Some(ToolSettlement {
                    origin,
                    execution_kind,
                    admitted_sequence,
                    started_sequence,
                    finished_sequence: sequence,
                    operation: operation.clone(),
                    outcome: outcome.clone(),
                })
            }
        };
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("tool audit sequence exhausted"))?;
        Ok(settlement)
    }
}

fn operation_key(operation: &ToolOperation) -> io::Result<String> {
    // JSON tuples avoid delimiter collisions and remain valid JSON map keys in
    // serialized checkpoints of this view. The tool name is checked separately.
    serde_json::to_string(&(
        &operation.thread_id,
        &operation.turn_id,
        &operation.call_id,
        &operation.source,
    ))
    .map_err(io::Error::other)
}
