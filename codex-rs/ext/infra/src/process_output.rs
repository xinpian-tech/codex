use std::io;
use std::io::Write;

use codex_exec_server::ProcessId;
use codex_exec_server_protocol::ExecOutputStream;
use codex_infra_state::JournalReader;

use super::ProcessAudit;
use super::ProcessAuditEvent;

/// Streams one launch attempt's complete journal output into caller-owned
/// sinks. Construct before starting the chosen process ID. Call from a blocking
/// worker; live process buffers and notification capacity do not affect it.
pub struct RecordedProcessOutput {
    audit: ProcessAudit,
    reader: JournalReader,
    process_id: ProcessId,
    requested: Option<u64>,
    accepted: bool,
    next_output: u64,
    exit_code: Option<i32>,
    closed: bool,
    failure: Option<String>,
}

impl ProcessAudit {
    pub fn output_reader(&self, process_id: ProcessId) -> io::Result<RecordedProcessOutput> {
        let writer = self
            .writer
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if let Some(error) = &writer.failure {
            return Err(io::Error::other(error.clone()));
        }
        let reader = JournalReader::open(&self.path, writer.journal.position())?;
        Ok(RecordedProcessOutput {
            audit: self.clone(),
            reader,
            process_id,
            requested: None,
            accepted: false,
            next_output: 1,
            exit_code: None,
            closed: false,
            failure: None,
        })
    }
}

impl RecordedProcessOutput {
    /// Returns the exit code only after start acceptance, Exited, and Closed.
    /// None means another poll is needed. Keep the same sinks across polls;
    /// a sink or journal error is terminal for this reader to avoid duplicating
    /// a partially written chunk on retry.
    pub fn drain_into(
        &mut self,
        stdout: &mut impl Write,
        stderr: &mut impl Write,
    ) -> io::Result<Option<i32>> {
        if let Some(error) = &self.failure {
            return Err(io::Error::other(error.clone()));
        }
        let result = self.drain(stdout, stderr);
        if let Err(error) = &result {
            self.failure = Some(error.to_string());
        }
        result
    }

    fn drain(
        &mut self,
        stdout: &mut impl Write,
        stderr: &mut impl Write,
    ) -> io::Result<Option<i32>> {
        let watermark = {
            let writer = self
                .audit
                .writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            if let Some(error) = &writer.failure {
                return Err(io::Error::other(error.clone()));
            }
            writer.journal.next_sequence()
        };
        // Only consume the acknowledged prefix, even if a concurrent writer
        // has produced a complete frame whose fsync has not finished yet.
        while self.reader.position().next_sequence < watermark {
            let record = self
                .reader
                .next_record()?
                .ok_or_else(|| io::Error::other("durable process output record missing"))?;
            let event: ProcessAuditEvent = serde_json::from_slice(&record.payload)?;
            match event {
                ProcessAuditEvent::Requested { params } => {
                    if params.process_id == self.process_id && self.requested.is_none() {
                        self.requested = Some(record.sequence);
                    }
                }
                ProcessAuditEvent::StartFinished {
                    requested_sequence,
                    outcome,
                } => {
                    if self.requested == Some(requested_sequence) {
                        let response = outcome.map_err(|error| {
                            io::Error::other(format!("recorded process start failed: {error:?}"))
                        })?;
                        if self.accepted || response.process_id != self.process_id {
                            return Err(io::Error::other(
                                "recorded process start identity or phase mismatch",
                            ));
                        }
                        self.accepted = true;
                    }
                }
                ProcessAuditEvent::Output {
                    requested_sequence,
                    process_id,
                    chunk,
                } => {
                    if self.matches(requested_sequence, &process_id)? {
                        self.advance(chunk.seq)?;
                        match chunk.stream {
                            ExecOutputStream::Stdout | ExecOutputStream::Pty => {
                                stdout.write_all(&chunk.chunk.0)?
                            }
                            ExecOutputStream::Stderr => stderr.write_all(&chunk.chunk.0)?,
                        }
                    }
                }
                ProcessAuditEvent::Exited {
                    requested_sequence,
                    process_id,
                    seq,
                    exit_code,
                    ..
                } => {
                    if self.matches(requested_sequence, &process_id)? {
                        self.advance(seq)?;
                        if self.exit_code.replace(exit_code).is_some() {
                            return Err(io::Error::other("recorded process exited twice"));
                        }
                    }
                }
                ProcessAuditEvent::Closed {
                    requested_sequence,
                    process_id,
                    seq,
                } => {
                    if self.matches(requested_sequence, &process_id)? {
                        self.advance(seq)?;
                        if self.exit_code.is_none() {
                            return Err(io::Error::other("recorded process closed without exit"));
                        }
                        self.closed = true;
                    }
                }
                ProcessAuditEvent::Failed {
                    requested_sequence,
                    process_id,
                    message,
                } => {
                    if self.matches(requested_sequence, &process_id)? {
                        return Err(io::Error::other(message));
                    }
                }
                ProcessAuditEvent::Opened { .. }
                | ProcessAuditEvent::Prepared { .. }
                | ProcessAuditEvent::InputRequested { .. }
                | ProcessAuditEvent::InputCloseRequested { .. }
                | ProcessAuditEvent::InputFinished { .. } => {}
            }
        }
        if self.accepted && self.closed {
            stdout.flush()?;
            stderr.flush()?;
            Ok(self.exit_code)
        } else {
            Ok(None)
        }
    }

    fn matches(&self, requested_sequence: Option<u64>, process_id: &ProcessId) -> io::Result<bool> {
        if process_id == &self.process_id && requested_sequence.is_none() {
            return Err(io::Error::other(
                "recorded process output has no launch attribution",
            ));
        }
        if self.requested.is_some() && requested_sequence == self.requested {
            if process_id != &self.process_id {
                return Err(io::Error::other("recorded process output identity changed"));
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn advance(&mut self, sequence: u64) -> io::Result<()> {
        if self.closed || sequence != self.next_output {
            return Err(io::Error::other(
                "recorded process output sequence mismatch",
            ));
        }
        self.next_output = sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("recorded process output sequence exhausted"))?;
        Ok(())
    }
}
