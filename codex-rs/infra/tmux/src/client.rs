use std::ffi::OsStr;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PanePlacement {
    pub session: String,
    pub window_id: String,
    pub pane_id: String,
}

pub struct AgentLaunch {
    pub agent_id: AgentId,
    pub launch_id: MessageId,
    pub worktree: PathBuf,
    pub host_program: PathBuf,
    pub host_args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaneProcessState {
    Alive,
    Exited,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaneProcess {
    pub placement: PanePlacement,
    pub pid: u32,
    pub state: PaneProcessState,
}

/// A machine runtime supplies the tmux executable, configuration and keeper
/// program from its Nix generation. The socket path selects its tmux server.
pub struct TmuxClient {
    program: PathBuf,
    socket: PathBuf,
    config: PathBuf,
    keeper: PathBuf,
}

impl TmuxClient {
    pub fn new(program: PathBuf, socket: PathBuf, config: PathBuf, keeper: PathBuf) -> Self {
        Self {
            program,
            socket,
            config,
            keeper,
        }
    }

    /// The keeper is Nix's `sleep`; its pane keeps the Session alive while Agent
    /// panes independently start and finish.
    pub fn ensure_session(&self, root_session_id: RootSessionId) -> io::Result<String> {
        let session = format!("codex-{root_session_id}");
        let target = format!("={session}");
        if self
            .command(["has-session", "-t", &target])
            .output()?
            .status
            .success()
        {
            return Ok(session);
        }
        self.run([
            OsStr::new("new-session"),
            OsStr::new("-d"),
            OsStr::new("-s"),
            OsStr::new(&session),
            OsStr::new("-n"),
            OsStr::new("runtime"),
            OsStr::new("--"),
            self.keeper.as_os_str(),
            OsStr::new("infinity"),
        ])?;
        Ok(session)
    }

    /// Only process bindings belong in argv. Task text and Bootstrap arrive
    /// through the new pane's stdin after placement and collection are ready.
    pub fn create_agent(&self, session: &str, launch: &AgentLaunch) -> io::Result<PanePlacement> {
        if let Some(existing) = self.inspect_launch(session, launch.agent_id, launch.launch_id)? {
            return match existing.state {
                PaneProcessState::Alive => Ok(existing.placement),
                PaneProcessState::Exited => Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "launch already exited; restart with a new launch ID",
                )),
            };
        }
        if launch.host_args.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Agent host requires binding arguments for direct tmux exec",
            ));
        }
        let target = format!("{session}:");
        let agent_name = format!("{}:{}", launch.agent_id, launch.launch_id);
        let mut command = self.command([
            OsStr::new("new-window"),
            OsStr::new("-d"),
            OsStr::new("-t"),
            OsStr::new(&target),
            OsStr::new("-n"),
            OsStr::new(&agent_name),
            OsStr::new("-c"),
            launch.worktree.as_os_str(),
            OsStr::new("-P"),
            OsStr::new("-F"),
            OsStr::new("#{window_id}\t#{pane_id}"),
            OsStr::new("--"),
            launch.host_program.as_os_str(),
        ]);
        let output = command.args(&launch.host_args).output()?;
        if !output.status.success() {
            return Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        let identifiers = std::str::from_utf8(&output.stdout)
            .map_err(io::Error::other)?
            .trim();
        let (window_id, pane_id) = identifiers
            .split_once('\t')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "tmux placement response"))?;
        Ok(PanePlacement {
            session: session.to_owned(),
            window_id: window_id.to_owned(),
            pane_id: pane_id.to_owned(),
        })
    }

    /// The explicit window name is the process incarnation's binding. `-n`
    /// disables automatic rename for this window (tmux's documented behavior).
    /// The launcher serializes create/reconcile for each Agent.
    pub fn inspect_launch(
        &self,
        session: &str,
        agent_id: AgentId,
        launch_id: MessageId,
    ) -> io::Result<Option<PaneProcess>> {
        let name = format!("{agent_id}:{launch_id}");
        let target = format!("={session}");
        let bytes = self.run([
            "list-panes",
            "-s",
            "-t",
            &target,
            "-F",
            "#{window_name}\t#{window_id}\t#{pane_id}\t#{pane_pid}\t#{pane_dead}",
        ])?;
        let output = std::str::from_utf8(&bytes).map_err(io::Error::other)?;
        let mut found = None;
        for line in output.lines() {
            let fields: Vec<_> = line.split('\t').collect();
            if fields.first().copied() != Some(name.as_str()) {
                continue;
            }
            if fields.len() != 5 || found.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "tmux launch must identify exactly one pane",
                ));
            }
            let state = match fields[4] {
                "0" => PaneProcessState::Alive,
                "1" => PaneProcessState::Exited,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid tmux pane_dead field",
                    ));
                }
            };
            found = Some(PaneProcess {
                placement: PanePlacement {
                    session: session.to_owned(),
                    window_id: fields[1].to_owned(),
                    pane_id: fields[2].to_owned(),
                },
                pid: fields[3].parse().map_err(io::Error::other)?,
                state,
            });
        }
        Ok(found)
    }

    /// The collector owns all three streams, journals raw output continuously,
    /// and parses control notifications independently of network connections.
    pub fn attach_control(&self, session: &str) -> io::Result<Child> {
        self.command(["-C", "attach-session", "-t", session])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
    }

    /// Input bytes must be journaled by the caller before injection. The receiver
    /// enters raw/no-echo mode before its ready notification so frames reach stdin
    /// without terminal line editing or input echo becoming new outbound traffic.
    pub fn write_input(&self, pane_id: &str, bytes: &[u8]) -> io::Result<()> {
        let buffer = format!("codex-{}", MessageId::new());
        let mut loader = self
            .command(["load-buffer", "-b", &buffer, "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let written = match loader.stdin.take() {
            Some(mut stdin) => stdin.write_all(bytes),
            None => Err(io::Error::other("tmux buffer loader stdin missing")),
        };
        let output = loader.wait_with_output()?;
        written?;
        if !output.status.success() {
            return Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        match self.run(["paste-buffer", "-d", "-r", "-b", &buffer, "-t", pane_id]) {
            Ok(_) => Ok(()),
            Err(error) => {
                let _ = self.run(["delete-buffer", "-b", &buffer]);
                Err(error)
            }
        }
    }

    /// Called after the host finalizer and collector have persisted completion.
    pub fn close_window(&self, window_id: &str) -> io::Result<()> {
        self.run(["kill-window", "-t", window_id])?;
        Ok(())
    }

    fn command<I, S>(&self, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(&self.program);
        command
            .arg("-S")
            .arg(&self.socket)
            .arg("-f")
            .arg(&self.config)
            .args(args);
        command
    }

    fn run<I, S>(&self, args: I) -> io::Result<Vec<u8>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.command(args).output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "tmux exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(output.stdout)
    }
}
