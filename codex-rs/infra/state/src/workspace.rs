use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::CommitId;
use codex_infra_protocol::RootSessionId;
use serde::Deserialize;
use serde::Serialize;

/// Native paths belong to the machine that owns this workspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBinding {
    pub root_session_id: RootSessionId,
    pub agent_id: AgentId,
    pub repository: PathBuf,
    pub worktree: PathBuf,
    pub branch: String,
    pub remote: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub operation_id: String,
    pub before: CommitId,
    pub pushed_commit: CommitId,
}

/// The runtime serializes this Agent's operations and supplies Git from its Nix
/// generation. Push failures leave the commit in place for the next attempt.
pub struct GitWorkspace {
    git: PathBuf,
    binding: WorkspaceBinding,
}

impl GitWorkspace {
    pub fn prepare(
        git: PathBuf,
        repository: &Path,
        root_session_id: RootSessionId,
        agent_id: AgentId,
        base: &CommitId,
        remote: String,
    ) -> io::Result<Self> {
        let repository = fs::canonicalize(repository)?;
        let name = repository
            .file_name()
            .ok_or_else(|| io::Error::other("repository name missing"))?;
        let parent = Path::new("/tmp/codex")
            .join(root_session_id.to_string())
            .join(agent_id.to_string());
        fs::create_dir_all(&parent)?;
        let worktree = parent.join(name);
        let branch = format!("codex/{root_session_id}/{agent_id}");
        let workspace = Self {
            git,
            binding: WorkspaceBinding {
                root_session_id,
                agent_id,
                repository,
                worktree,
                branch,
                remote,
            },
        };
        workspace.command(
            &workspace.binding.repository,
            [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("-b"),
                OsStr::new(&workspace.binding.branch),
                OsStr::new("--"),
                workspace.binding.worktree.as_os_str(),
                OsStr::new(&base.to_string()),
            ],
        )?;
        Ok(workspace)
    }

    pub fn resume(git: PathBuf, binding: WorkspaceBinding) -> io::Result<Self> {
        let workspace = Self { git, binding };
        let actual = workspace.command(
            &workspace.binding.worktree,
            ["rev-parse", "--show-toplevel"],
        )?;
        let actual = std::str::from_utf8(&actual)
            .map_err(io::Error::other)?
            .trim();
        if fs::canonicalize(actual)? != fs::canonicalize(&workspace.binding.worktree)? {
            return Err(io::Error::other(
                "worktree binding does not match repository root",
            ));
        }
        workspace.current_commit()?;
        Ok(workspace)
    }

    pub fn binding(&self) -> &WorkspaceBinding {
        &self.binding
    }

    pub fn current_commit(&self) -> io::Result<CommitId> {
        let branch = self.command(
            &self.binding.worktree,
            ["symbolic-ref", "--quiet", "--short", "HEAD"],
        )?;
        if std::str::from_utf8(&branch)
            .map_err(io::Error::other)?
            .trim()
            != self.binding.branch
        {
            return Err(io::Error::other("Agent worktree is on a different branch"));
        }
        let head = self.command(&self.binding.worktree, ["rev-parse", "HEAD"])?;
        std::str::from_utf8(&head)
            .map_err(io::Error::other)?
            .trim()
            .parse()
            .map_err(io::Error::other)
    }

    /// Runs for each completed mutation and for finalization, including when the
    /// worktree is clean. The caller journals pending/completed operation state.
    pub fn checkpoint(&self, operation_id: &str) -> io::Result<Checkpoint> {
        let before = self.current_commit()?;
        let status = self.command(
            &self.binding.worktree,
            ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        )?;
        if !status.is_empty() {
            self.command(&self.binding.worktree, ["add", "-A", "--", "."])?;
            let staged = self.command(
                &self.binding.worktree,
                ["diff", "--cached", "--name-only", "-z"],
            )?;
            if !staged.is_empty() {
                let message = format!("agent {} checkpoint {operation_id}", self.binding.agent_id);
                self.command(&self.binding.worktree, ["commit", "--message", &message])?;
            }
        }
        if !self
            .command(
                &self.binding.worktree,
                ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
            )?
            .is_empty()
        {
            return Err(io::Error::other(
                "workspace still has changes after checkpoint commit",
            ));
        }
        let pushed_commit = self.current_commit()?;
        let destination = format!("refs/heads/{}", self.binding.branch);
        let refspec = format!("{pushed_commit}:{destination}");
        self.command(
            &self.binding.worktree,
            ["push", "--", &self.binding.remote, &refspec],
        )?;
        let advertised = self.command(
            &self.binding.worktree,
            [
                "ls-remote",
                "--refs",
                "--",
                &self.binding.remote,
                &destination,
            ],
        )?;
        let advertised = std::str::from_utf8(&advertised).map_err(io::Error::other)?;
        let expected = format!("{pushed_commit}\t{destination}");
        if !advertised.lines().any(|line| line == expected) {
            return Err(io::Error::other(
                "remote branch does not advertise checkpoint commit",
            ));
        }
        Ok(Checkpoint {
            operation_id: operation_id.to_owned(),
            before,
            pushed_commit,
        })
    }

    fn command<I, S>(&self, directory: &Path, args: I) -> io::Result<Vec<u8>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = Command::new(&self.git)
            .arg("-C")
            .arg(directory)
            .args(args)
            .output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "Git exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(output.stdout)
    }
}
