use std::ffi::OsStr;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::RootSessionId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveReceipt {
    pub session_ref: String,
    pub commit: CommitId,
    pub segment_name: String,
}

/// A machine's Session branch uses a dedicated Git index and one local writer.
/// Callers supply complete immutable chunks from durable spool. Remote receipt
/// is returned separately from the local journal's durability acknowledgement.
pub struct SessionShard {
    git: PathBuf,
    repository: PathBuf,
    index: PathBuf,
    remote: String,
    session_ref: String,
    _writer_lock: File,
}

impl SessionShard {
    pub(crate) fn session_ref(&self) -> &str {
        &self.session_ref
    }

    pub fn open(
        git: PathBuf,
        repository: PathBuf,
        index: PathBuf,
        remote: String,
        root_session_id: RootSessionId,
        machine_id: &MachineId,
    ) -> io::Result<Self> {
        if !index.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Session index must be absolute",
            ));
        }
        let writer_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(index.with_extension("writer-lock"))?;
        writer_lock.lock()?;
        Ok(Self {
            git,
            repository,
            index,
            remote,
            session_ref: format!("refs/codex/session-shards/{root_session_id}/{machine_id}"),
            _writer_lock: writer_lock,
        })
    }

    /// Publishes content-addressed chunks; replay of the same chunk is idempotent.
    /// Sequence/epoch metadata belongs inside the chunk's durable stream envelope.
    pub fn publish(&mut self, bytes: &[u8]) -> io::Result<ArchiveReceipt> {
        let segment_name = format!("segments/{}.bin", blake3::hash(bytes));
        let parent_output = self
            .command(["rev-parse", "--verify", "--quiet", &self.session_ref])
            .output()?;
        let parent = if parent_output.status.success() {
            Some(
                String::from_utf8(parent_output.stdout)
                    .map_err(io::Error::other)?
                    .trim()
                    .to_owned(),
            )
        } else if parent_output.status.code() == Some(1) {
            None
        } else {
            return Err(io::Error::other(
                String::from_utf8_lossy(&parent_output.stderr).into_owned(),
            ));
        };
        match &parent {
            Some(parent) => {
                self.run(["read-tree", parent])?;
            }
            None => {
                self.run(["read-tree", "--empty"])?;
            }
        }
        let mut hash = self
            .command(["hash-object", "-w", "--stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let input_result = match hash.stdin.take() {
            Some(mut stdin) => stdin.write_all(bytes),
            None => Err(io::Error::other("Git object writer stdin missing")),
        };
        let output = hash.wait_with_output()?;
        input_result?;
        if !output.status.success() {
            return Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        let object = String::from_utf8(output.stdout).map_err(io::Error::other)?;
        let entry = format!("100644,{},{}", object.trim(), segment_name);
        self.run(["update-index", "--add", "--cacheinfo", &entry])?;
        let tree = self.run(["write-tree"])?;
        let unchanged = match &parent {
            Some(parent) => self.run(["rev-parse", &format!("{parent}^{{tree}}")])? == tree,
            None => false,
        };
        let commit = match parent.as_ref().filter(|_| unchanged) {
            Some(parent) => parent.clone(),
            None => {
                let mut args = vec!["commit-tree", tree.trim()];
                if let Some(parent) = &parent {
                    args.extend(["-p", parent]);
                }
                args.extend(["-m", "Archive machine Session segment"]);
                let commit = self.run(args)?.trim().to_owned();
                let previous = parent
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| "0".repeat(commit.len()));
                self.run(["update-ref", &self.session_ref, &commit, &previous])?;
                commit
            }
        };
        let refspec = format!("{commit}:{}", self.session_ref);
        self.run(["push", "--", &self.remote, &refspec])?;
        let advertised =
            self.run(["ls-remote", "--refs", "--", &self.remote, &self.session_ref])?;
        let expected = format!("{commit}\t{}", self.session_ref);
        if !advertised.lines().any(|line| line == expected) {
            return Err(io::Error::other(
                "remote Session ref differs from published commit",
            ));
        }
        Ok(ArchiveReceipt {
            session_ref: self.session_ref.clone(),
            commit: commit.parse().map_err(io::Error::other)?,
            segment_name,
        })
    }

    fn command<I, S>(&self, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(&self.git);
        command
            .arg("-C")
            .arg(&self.repository)
            .args(args)
            .env("GIT_INDEX_FILE", &self.index);
        command
    }

    fn run<I, S>(&self, args: I) -> io::Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.command(args).output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "Git exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        String::from_utf8(output.stdout).map_err(io::Error::other)
    }
}
