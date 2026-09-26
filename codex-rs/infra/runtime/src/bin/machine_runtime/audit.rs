use std::fs;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;

use codex_infra_protocol::MessageId;
use codex_infra_runtime::MachineLaunchConfig;
use codex_infra_runtime::MachineLaunchProvenance;
use codex_infra_state::Journal;
use serde::Serialize;
use tokio::sync::mpsc;

use super::Output;
use super::Request;

pub(super) struct ControlAudit {
    pub(super) run_id: MessageId,
    output: Journal,
    lifecycle: Journal,
}

pub(super) struct InputAudit {
    bytes: Journal,
    lifecycle: Journal,
}

impl ControlAudit {
    pub(super) fn open(
        config: &MachineLaunchConfig,
        provenance: &MachineLaunchProvenance,
    ) -> io::Result<(Self, InputAudit)> {
        let run_id = MessageId::new();
        let directory = config
            .spool_directory
            .join("machine-control")
            .join(run_id.to_string());
        fs::create_dir_all(&directory)?;
        let mut audit = Self {
            run_id,
            output: Journal::open(&directory.join("stdout.journal"), |_| Ok(()))?,
            lifecycle: Journal::open(&directory.join("lifecycle.journal"), |_| Ok(()))?,
        };
        let input = InputAudit {
            bytes: Journal::open(&directory.join("stdin.journal"), |_| Ok(()))?,
            lifecycle: Journal::open(&directory.join("stdin-lifecycle.journal"), |_| Ok(()))?,
        };
        audit.event(
            "opened",
            &serde_json::json!({
                "run_id": run_id,
                "root_session_id": config.root_session_id,
                "expected_machine_id": config.machine_id,
                "generation": provenance,
                "executable": std::env::current_exe()?,
            }),
        )?;
        Ok((audit, input))
    }

    pub(super) fn event(&mut self, event: &str, detail: &impl Serialize) -> io::Result<()> {
        self.lifecycle.append(&serde_json::to_vec(
            &serde_json::json!({"event": event, "detail": detail}),
        )?)?;
        Ok(())
    }

    /// Record exact intended output before terminal delivery. The lifecycle
    /// records delivery failures separately from these locally durable bytes.
    pub(super) fn emit(&mut self, output: Output) -> io::Result<()> {
        let mut bytes = serde_json::to_vec(&output)?;
        bytes.push(b'\n');
        self.output.append(&bytes)?;
        let result = {
            let mut stdout = io::stdout().lock();
            stdout.write_all(&bytes).and_then(|()| stdout.flush())
        };
        if let Err(error) = &result {
            self.event("stdout_failed", &error.to_string())?;
        }
        result
    }
}

impl InputAudit {
    /// Captures bytes underneath buffering and JSON parsing, including invalid
    /// UTF-8 and partial requests. An input reader blocked on stdin is not marked
    /// complete merely because the machine received a stop signal elsewhere.
    pub(super) fn read_requests(self, sender: mpsc::Sender<io::Result<Request>>) {
        let mut reader = BufReader::new(RecordedInput {
            inner: io::stdin().lock(),
            audit: self,
        });
        loop {
            let mut line = Vec::new();
            let request = match reader.read_until(b'\n', &mut line) {
                Ok(0) => break,
                Ok(_) => serde_json::from_slice(&line).map_err(io::Error::other),
                Err(error) => Err(error),
            };
            let failed = request.is_err();
            if sender.blocking_send(request).is_err() || failed {
                break;
            }
        }
    }
}

struct RecordedInput<R> {
    inner: R,
    audit: InputAudit,
}

impl<R: Read> Read for RecordedInput<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        match self.inner.read(bytes) {
            Ok(0) => {
                self.audit.lifecycle.append(b"eof")?;
                Ok(0)
            }
            Ok(length) => {
                self.audit.bytes.append(&bytes[..length])?;
                Ok(length)
            }
            Err(error) => {
                self.audit
                    .lifecycle
                    .append(format!("read_failed: {error}").as_bytes())?;
                Err(error)
            }
        }
    }
}
