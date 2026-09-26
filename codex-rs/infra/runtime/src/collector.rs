use std::fs;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::ChildStdin;
use std::process::ExitStatus;
use std::thread;
use std::thread::JoinHandle;

use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_tmux::TmuxClient;

mod archive;
mod archive_actor;
mod archive_worker;
mod completion;
mod progress;
pub use archive::CollectorArchiveJobIds;
pub use archive::CollectorArchiveJobs;
pub use archive_actor::CollectorArchiveActor;
pub use archive_actor::CollectorArchiveCompletion;
pub use archive_worker::CollectorArchiveWorker;
pub use completion::CollectorFinished;

/// One attachment has its own streams so a new control connection never joins
/// the unfinished final line of a previous connection during journal replay.
pub struct ControlCollector {
    attachment_id: MessageId,
    directory: PathBuf,
    child: Child,
    input: Option<ChildStdin>,
    commands: Journal,
    lifecycle: Journal,
    stdout: JoinHandle<io::Result<CapturedStream>>,
    stderr: JoinHandle<io::Result<CapturedStream>>,
}

#[derive(Debug)]
pub struct CollectorCompletion {
    pub status: ExitStatus,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_position: JournalPosition,
    pub stderr_position: JournalPosition,
    pub commands_position: JournalPosition,
    pub lifecycle_position: JournalPosition,
}

impl ControlCollector {
    /// The runtime starts collection before releasing Agent startup. Capture
    /// threads only drain pipes to disk; gateways consume the journals separately.
    pub fn attach(client: &TmuxClient, session: &str, parent: &Path) -> io::Result<Self> {
        let attachment_id = MessageId::new();
        let directory = parent.join(attachment_id.to_string());
        fs::create_dir_all(&directory)?;
        let directory = directory.canonicalize()?;
        let stdout_journal = Journal::open(&directory.join("stdout.journal"), |_| Ok(()))?;
        let stderr_journal = Journal::open(&directory.join("stderr.journal"), |_| Ok(()))?;
        let commands = Journal::open(&directory.join("stdin.journal"), |_| Ok(()))?;
        let mut lifecycle = Journal::open(&directory.join("lifecycle.journal"), |_| Ok(()))?;
        lifecycle.append(format!("attach {session}").as_bytes())?;
        for (name, position) in [
            ("stdout", stdout_journal.position()),
            ("stderr", stderr_journal.position()),
            ("stdin", commands.position()),
            ("lifecycle", lifecycle.position()),
        ] {
            progress::publish(&directory, name, attachment_id, position)?;
        }
        let mut child = client.attach_control(session)?;
        let input = child.stdin.take();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("control stdout missing"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("control stderr missing"))?;
        let stdout_directory = directory.clone();
        let stdout = thread::spawn(move || {
            capture(
                stdout,
                stdout_journal,
                &stdout_directory,
                "stdout",
                attachment_id,
            )
        });
        let stderr_directory = directory.clone();
        let stderr = thread::spawn(move || {
            capture(
                stderr,
                stderr_journal,
                &stderr_directory,
                "stderr",
                attachment_id,
            )
        });
        Ok(Self {
            attachment_id,
            directory,
            child,
            input,
            commands,
            lifecycle,
            stdout,
            stderr,
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// A worker ending while the control client remains alive also needs runtime
    /// attention: that pipe is no longer being captured. `finish` returns the
    /// concrete recording error after detaching the control client.
    pub fn needs_attention(&mut self) -> io::Result<bool> {
        Ok(self.child.try_wait()?.is_some()
            || self.stdout.is_finished()
            || self.stderr.is_finished())
    }

    /// Detaches only this observer. Agent panes and the Session keep running.
    /// The runtime reconnects collection independently of gateway TCP connections.
    pub fn detach(&mut self) -> io::Result<()> {
        if let Some(mut input) = self.input.take() {
            let bytes = b"detach-client\n";
            self.commands.append(bytes)?;
            input.write_all(bytes)?;
            input.flush()?;
            progress::publish(
                &self.directory,
                "stdin",
                self.attachment_id,
                self.commands.position(),
            )?;
        }
        Ok(())
    }

    /// Call after client exit or `detach`. Both pipes drain through EOF before
    /// the final lifecycle record identifies their complete byte counts.
    pub fn finish(mut self) -> io::Result<CollectorCompletion> {
        let status = self.child.wait()?;
        let stdout = self
            .stdout
            .join()
            .map_err(|_| io::Error::other("control stdout recorder panicked"));
        let stderr = self
            .stderr
            .join()
            .map_err(|_| io::Error::other("control stderr recorder panicked"));
        self.lifecycle
            .append(format!("exit {status}; stdout={stdout:?}; stderr={stderr:?}").as_bytes())?;
        progress::publish(
            &self.directory,
            "lifecycle",
            self.attachment_id,
            self.lifecycle.position(),
        )?;
        let stdout = stdout??;
        let stderr = stderr??;
        let completed = CollectorCompletion {
            status,
            stdout_bytes: stdout.bytes,
            stderr_bytes: stderr.bytes,
            stdout_position: stdout.position,
            stderr_position: stderr.position,
            commands_position: self.commands.position(),
            lifecycle_position: self.lifecycle.position(),
        };
        CollectorFinished {
            attachment_id: self.attachment_id,
            exit_status: status.to_string(),
            exit_code: status.code(),
            success: status.success(),
            stdout_bytes: completed.stdout_bytes,
            stderr_bytes: completed.stderr_bytes,
            stdout_position: completed.stdout_position,
            stderr_position: completed.stderr_position,
            commands_position: completed.commands_position,
            lifecycle_position: completed.lifecycle_position,
        }
        .persist(&self.directory)?;
        Ok(completed)
    }
}

#[derive(Debug)]
struct CapturedStream {
    bytes: u64,
    position: JournalPosition,
}

fn capture(
    mut stream: impl Read,
    mut journal: Journal,
    directory: &Path,
    name: &str,
    attachment_id: MessageId,
) -> io::Result<CapturedStream> {
    let mut bytes = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let length = match stream.read(&mut bytes) {
            Ok(length) => length,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if length == 0 {
            return Ok(CapturedStream {
                bytes: total,
                position: journal.position(),
            });
        }
        journal.append(&bytes[..length])?;
        progress::publish(directory, name, attachment_id, journal.position())?;
        total = total
            .checked_add(length as u64)
            .ok_or_else(|| io::Error::other("control stream offset exhausted"))?;
    }
}
