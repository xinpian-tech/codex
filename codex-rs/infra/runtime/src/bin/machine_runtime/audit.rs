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
use codex_infra_state::JournalPosition;
use serde::Serialize;
use tokio::sync::mpsc;

use super::Output;
use super::Request;
use super::archive::ControlArchive;
use super::input_worker::InputStopped;

#[derive(Serialize)]
pub(super) struct InputCompletion {
    input: JournalPosition,
    lifecycle: JournalPosition,
}

pub(super) struct ControlAudit {
    pub(super) run_id: MessageId,
    archive: ControlArchive,
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
        let directory = directory.canonicalize()?;
        let mut audit = Self {
            run_id,
            archive: ControlArchive {
                directory: directory.clone(),
                root_session_id: config.root_session_id,
                machine_id: config.machine_id.clone(),
                run_id,
            },
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

    pub(super) fn finish(
        mut self,
        input: &io::Result<InputCompletion>,
    ) -> io::Result<Vec<codex_infra_runtime::ArchiveJob>> {
        self.event("control_closed", &())?;
        let mut positions = vec![
            ("stdout", self.output.position()),
            ("lifecycle", self.lifecycle.position()),
        ];
        if let Ok(input) = input {
            positions.push(("stdin", input.input));
            positions.push(("stdin-lifecycle", input.lifecycle));
        }
        let Self { archive, .. } = self;
        archive.prepare(&positions)
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
    /// No reader was started, so these positions describe the empty input
    /// producer and its explicit not-started lifecycle, rather than stdin EOF.
    pub(super) fn close_unstarted(mut self) -> io::Result<InputCompletion> {
        self.lifecycle.append(b"reader_not_started")?;
        Ok(InputCompletion {
            input: self.bytes.position(),
            lifecycle: self.lifecycle.position(),
        })
    }

    /// Captures bytes underneath buffering and JSON parsing, including invalid
    /// UTF-8 and partial requests. An input reader blocked on stdin is not marked
    /// complete merely because the machine received a stop signal elsewhere.
    pub(super) fn read_requests(
        self,
        sender: mpsc::Sender<io::Result<Request>>,
        input: impl Read,
    ) -> io::Result<InputCompletion> {
        let mut reader = BufReader::new(RecordedInput {
            inner: input,
            audit: self,
        });
        loop {
            let mut line = Vec::new();
            let request = match reader.read_until(b'\n', &mut line) {
                Ok(0) => break,
                Ok(_) => serde_json::from_slice(&line).map_err(io::Error::other),
                Err(error)
                    if error
                        .get_ref()
                        .is_some_and(<dyn std::error::Error + Send + Sync>::is::<InputStopped>) =>
                {
                    break;
                }
                Err(error) => Err(error),
            };
            let failed = request.is_err();
            if sender.blocking_send(request).is_err() || failed {
                break;
            }
        }
        let mut recorded = reader.into_inner();
        recorded.audit.lifecycle.append(b"reader_closed")?;
        Ok(InputCompletion {
            input: recorded.audit.bytes.position(),
            lifecycle: recorded.audit.lifecycle.position(),
        })
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
                if error
                    .get_ref()
                    .is_some_and(<dyn std::error::Error + Send + Sync>::is::<InputStopped>)
                {
                    self.audit.lifecycle.append(b"stop_requested")?;
                } else {
                    self.audit
                        .lifecycle
                        .append(format!("read_failed: {error}").as_bytes())?;
                }
                Err(error)
            }
        }
    }
}
