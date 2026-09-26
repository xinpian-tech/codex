use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;

use codex_exec_server::ProcessId;
use codex_exec_server_protocol::ExecMetadata;
use codex_exec_server_protocol::JSONRPCErrorError;
use codex_infra_protocol::MessageId;
use serde::Deserialize;
use serde::Serialize;

use crate::ProcessAuditEvent;
use crate::StoreAuditIdentity;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Attempt {
    process_id: ProcessId,
    metadata: Option<ExecMetadata>,
    accepted: bool,
    next_output_sequence: u64,
    exit_code: Option<i32>,
    closed: bool,
}

/// One start attempt whose recorded request and producer lifecycle settled.
/// This is an input to operation reconciliation, not permission to checkpoint:
/// tool completion, other writers and mutation serialization remain separate.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessSettlement {
    pub requested_sequence: u64,
    pub process_id: ProcessId,
    pub metadata: Option<ExecMetadata>,
    pub outcome: ProcessSettlementOutcome,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcessSettlementOutcome {
    StartFailed { error: JSONRPCErrorError },
    Closed { exit_code: i32 },
}

/// Replayable view of one launch journal. Only unresolved identifiers and
/// attribution stay in memory; command, stdin and output bodies stay on disk.
/// Persist this view and the reader cursor only after handling a settlement.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProcessActivity {
    next_sequence: u64,
    binding: Option<(StoreAuditIdentity, MessageId)>,
    attempts: BTreeMap<u64, Attempt>,
    inputs: BTreeSet<u64>,
    failure: Option<String>,
}

impl ProcessActivity {
    pub fn apply(
        &mut self,
        sequence: u64,
        event: &ProcessAuditEvent,
    ) -> io::Result<Option<ProcessSettlement>> {
        if let Some(failure) = &self.failure {
            return Err(io::Error::other(failure.clone()));
        }
        let result = self.apply_next(sequence, event);
        if let Err(error) = &result {
            self.failure = Some(error.to_string());
        }
        result
    }

    /// A caller must also have reached the writer's required journal watermark.
    /// An empty prefix alone does not establish that no work is running.
    pub fn require_settled(&self) -> io::Result<()> {
        if let Some(failure) = &self.failure {
            return Err(io::Error::other(failure.clone()));
        }
        if self.binding.is_none() || !self.attempts.is_empty() || !self.inputs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "process activity is unresolved",
            ));
        }
        Ok(())
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    fn attempt(
        &mut self,
        sequence: Option<u64>,
        process_id: &ProcessId,
    ) -> io::Result<(u64, &mut Attempt)> {
        let sequence = sequence.ok_or_else(|| {
            io::Error::other("legacy process record has unknown start attribution")
        })?;
        let attempt = self
            .attempts
            .get_mut(&sequence)
            .ok_or_else(|| io::Error::other("process event has no pending start"))?;
        if &attempt.process_id != process_id {
            return Err(io::Error::other("process event start identity mismatch"));
        }
        Ok((sequence, attempt))
    }

    fn apply_next(
        &mut self,
        sequence: u64,
        event: &ProcessAuditEvent,
    ) -> io::Result<Option<ProcessSettlement>> {
        if sequence != self.next_sequence {
            return Err(io::Error::other("process audit sequence discontinuity"));
        }
        if self.binding.is_none() && !matches!(event, ProcessAuditEvent::Opened { .. }) {
            return Err(io::Error::other("process audit opening record missing"));
        }
        let mut candidate = None;
        let mut settled = None;
        match event {
            ProcessAuditEvent::Opened {
                identity,
                launch_id,
            } => {
                let binding = (identity.clone(), *launch_id);
                if self
                    .binding
                    .as_ref()
                    .is_some_and(|previous| previous != &binding)
                {
                    return Err(io::Error::other("process audit binding changed"));
                }
                self.binding = Some(binding);
            }
            ProcessAuditEvent::Requested { params } => {
                self.attempts.insert(
                    sequence,
                    Attempt {
                        process_id: params.process_id.clone(),
                        metadata: params.metadata.clone(),
                        accepted: false,
                        next_output_sequence: 1,
                        exit_code: None,
                        closed: false,
                    },
                );
            }
            ProcessAuditEvent::StartFinished {
                requested_sequence,
                outcome,
            } => {
                let attempt = self
                    .attempts
                    .get_mut(requested_sequence)
                    .ok_or_else(|| io::Error::other("start outcome has no pending request"))?;
                if attempt.accepted {
                    return Err(io::Error::other("start outcome already recorded"));
                }
                match outcome {
                    Ok(response) => {
                        if response.process_id != attempt.process_id {
                            return Err(io::Error::other(
                                "start response process identity mismatch",
                            ));
                        }
                        attempt.accepted = true;
                        candidate = Some(*requested_sequence);
                    }
                    Err(error) => {
                        if attempt.next_output_sequence != 1 {
                            return Err(io::Error::other("failed start has producer activity"));
                        }
                        settled = Some(ProcessSettlement {
                            requested_sequence: *requested_sequence,
                            process_id: attempt.process_id.clone(),
                            metadata: attempt.metadata.clone(),
                            outcome: ProcessSettlementOutcome::StartFailed {
                                error: error.clone(),
                            },
                        });
                        self.attempts.remove(requested_sequence);
                    }
                }
            }
            ProcessAuditEvent::Prepared {
                requested_sequence,
                process_id,
                ..
            } => {
                self.attempt(*requested_sequence, process_id)?;
            }
            ProcessAuditEvent::InputRequested { .. }
            | ProcessAuditEvent::InputCloseRequested { .. } => {
                self.inputs.insert(sequence);
            }
            ProcessAuditEvent::InputFinished {
                requested_sequence, ..
            } => {
                if !self.inputs.remove(requested_sequence) {
                    return Err(io::Error::other("stdin outcome has no pending request"));
                }
            }
            ProcessAuditEvent::Output {
                requested_sequence,
                process_id,
                chunk,
            } => {
                let (_, attempt) = self.attempt(*requested_sequence, process_id)?;
                if attempt.closed || chunk.seq != attempt.next_output_sequence {
                    return Err(io::Error::other("process output sequence mismatch"));
                }
                attempt.next_output_sequence += 1;
            }
            ProcessAuditEvent::Exited {
                requested_sequence,
                process_id,
                seq,
                exit_code,
                ..
            } => {
                let (_, attempt) = self.attempt(*requested_sequence, process_id)?;
                if attempt.exit_code.is_some() || *seq != attempt.next_output_sequence {
                    return Err(io::Error::other("process exit sequence mismatch"));
                }
                attempt.exit_code = Some(*exit_code);
                attempt.next_output_sequence += 1;
            }
            ProcessAuditEvent::Closed {
                requested_sequence,
                process_id,
                seq,
            } => {
                let (sequence, attempt) = self.attempt(*requested_sequence, process_id)?;
                if attempt.closed
                    || attempt.exit_code.is_none()
                    || *seq != attempt.next_output_sequence
                {
                    return Err(io::Error::other("process close sequence mismatch"));
                }
                attempt.closed = true;
                attempt.next_output_sequence += 1;
                candidate = Some(sequence);
            }
            ProcessAuditEvent::Failed { message, .. } => {
                return Err(io::Error::other(format!(
                    "process producer failed: {message}"
                )));
            }
        }
        if let Some(sequence) = candidate
            && let Some(attempt) = self.attempts.get(&sequence)
            && attempt.accepted
            && attempt.closed
        {
            settled = Some(ProcessSettlement {
                requested_sequence: sequence,
                process_id: attempt.process_id.clone(),
                metadata: attempt.metadata.clone(),
                outcome: ProcessSettlementOutcome::Closed {
                    exit_code: attempt
                        .exit_code
                        .ok_or_else(|| io::Error::other("closed process has no exit status"))?,
                },
            });
            self.attempts.remove(&sequence);
        }
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("process audit sequence exhausted"))?;
        Ok(settled)
    }
}
