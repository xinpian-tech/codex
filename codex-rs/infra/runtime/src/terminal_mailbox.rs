use std::fs::File;
use std::io;
use std::io::Read;
use std::os::fd::AsFd;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_tmux::AgentTerminal;
use serde::Serialize;
use tokio::sync::watch;

use crate::HostMailbox;
use crate::LaunchIntent;

/// Input progress is notification metadata; message bodies stay in HostMailbox.
#[derive(Clone, Debug)]
pub enum TerminalInputState {
    Reading,
    Updated { chunks: u64 },
    Stopped { error: Option<String> },
}

/// Owns the Agent's raw terminal and an independent, interruptible stdin reader.
/// Use `with_mailbox` to serialize local operations with incoming tmux frames.
pub struct TerminalMailbox {
    shared: Arc<Mutex<Option<HostMailbox<io::Stdout>>>>,
    state: watch::Receiver<TerminalInputState>,
    stop: UnixStream,
    task: thread::JoinHandle<io::Result<JournalPosition>>,
    terminal: Arc<Mutex<AgentTerminal>>,
}

/// Input has ended. The finalizer can still send/flush the last checkpointed
/// messages before explicitly restoring the terminal and archiving its tails.
pub struct TerminalMailboxExit {
    pub mailbox: HostMailbox<io::Stdout>,
    terminal: Arc<Mutex<AgentTerminal>>,
    pub input_lifecycle: JournalPosition,
    pub input_error: Option<String>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum InputEvent<'a> {
    Opened {
        launch: &'a LaunchIntent,
    },
    Finished {
        reason: &'a str,
        error: Option<&'a str>,
    },
}

impl TerminalMailboxExit {
    /// Restore after the finalizer has flushed its last terminal output.
    pub fn restore_terminal(&mut self) -> io::Result<()> {
        self.terminal
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?
            .restore()
    }
}

impl TerminalMailbox {
    /// Call on a blocking worker. Readiness is emitted only after raw mode and
    /// durable mailbox recovery, through this Agent's own terminal stdout.
    pub fn open(directory: &Path, launch: &LaunchIntent) -> io::Result<Self> {
        let mut input = File::from(io::stdin().as_fd().try_clone_to_owned()?);
        let terminal = Arc::new(Mutex::new(AgentTerminal::enter(&input)?));
        let mut mailbox = HostMailbox::open(
            directory,
            launch.workspace.root_session_id,
            launch.workspace.agent_id,
            io::stdout(),
        )?;
        let mut lifecycle = Journal::open(&directory.join("input-lifecycle.journal"), |_| Ok(()))?;
        lifecycle.append(&serde_json::to_vec(&InputEvent::Opened { launch })?)?;
        let (stop, wake) = UnixStream::pair()?;
        mailbox.announce_ready(launch.launch_id)?;
        mailbox.flush()?;
        let shared = Arc::new(Mutex::new(Some(mailbox)));
        let worker_mailbox = Arc::clone(&shared);
        let worker_terminal = Arc::clone(&terminal);
        let (state, receiver) = watch::channel(TerminalInputState::Reading);
        let task = thread::Builder::new()
            .name("agent-tmux-stdin".to_owned())
            .spawn(move || {
                let _terminal = worker_terminal;
                let mut chunks = 0_u64;
                let mut bytes = [0_u8; 8192];
                let result = (|| -> io::Result<&str> {
                    loop {
                        if !wait_input(&input, &wake)? {
                            return Ok("stop_requested");
                        }
                        let count = match input.read(&mut bytes) {
                            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                            result => result?,
                        };
                        if count == 0 {
                            return Ok("eof");
                        }
                        let mut guard = worker_mailbox
                            .lock()
                            .map_err(|error| io::Error::other(error.to_string()))?;
                        let mailbox = guard
                            .as_mut()
                            .ok_or_else(|| io::Error::other("terminal mailbox handed off"))?;
                        mailbox.receive_stdin(&bytes[..count])?;
                        mailbox.flush()?;
                        chunks = chunks
                            .checked_add(1)
                            .ok_or_else(|| io::Error::other("terminal chunk count exhausted"))?;
                        state.send_replace(TerminalInputState::Updated { chunks });
                    }
                })();
                let error = result.as_ref().err().map(ToString::to_string);
                let recorded = lifecycle.append(&serde_json::to_vec(&InputEvent::Finished {
                    reason: result.as_ref().copied().unwrap_or("input_error"),
                    error: error.as_deref(),
                })?);
                state.send_replace(TerminalInputState::Stopped {
                    error: match &recorded {
                        Ok(_) => error,
                        Err(failure) => Some(format!("input: {error:?}; lifecycle: {failure}")),
                    },
                });
                recorded?;
                Ok(lifecycle.position())
            })?;
        Ok(Self {
            shared,
            state: receiver,
            stop,
            task,
            terminal,
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<TerminalInputState> {
        self.state.clone()
    }

    /// Runs file/terminal work outside the async executor. Cancellation of the
    /// waiter does not interrupt an already submitted mailbox operation.
    pub async fn with_mailbox<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut HostMailbox<io::Stdout>) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let shared = Arc::clone(&self.shared);
        let terminal = Arc::clone(&self.terminal);
        tokio::task::spawn_blocking(move || {
            let _terminal = terminal;
            let mut guard = shared
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            let mailbox = guard
                .as_mut()
                .ok_or_else(|| io::Error::other("terminal mailbox handed off"))?;
            let result = operation(mailbox);
            let flushed = mailbox.flush();
            result.and_then(|value| flushed.map(|()| value))
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Wakes stdin independently of terminal activity, joins its producer, then
    /// transfers ownership. Stop input only after deciding how outstanding peer
    /// receipts will be handled; it does not imply message delivery completion.
    pub async fn stop(self) -> io::Result<TerminalMailboxExit> {
        let Self {
            shared,
            state,
            stop,
            task,
            terminal,
        } = self;
        drop(stop);
        tokio::task::spawn_blocking(move || {
            let input_lifecycle = task
                .join()
                .map_err(|_| io::Error::other("terminal input panicked"))??;
            let input_error = match &*state.borrow() {
                TerminalInputState::Stopped { error } => error.clone(),
                TerminalInputState::Reading | TerminalInputState::Updated { .. } => {
                    return Err(io::Error::other("terminal input ended without completion"));
                }
            };
            let mailbox = shared
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?
                .take()
                .ok_or_else(|| io::Error::other("terminal mailbox already handed off"))?;
            Ok(TerminalMailboxExit {
                mailbox,
                terminal,
                input_lifecycle,
                input_error,
            })
        })
        .await
        .map_err(io::Error::other)?
    }
}

fn wait_input(input: &File, wake: &UnixStream) -> io::Result<bool> {
    loop {
        let mut fds = [
            libc::pollfd {
                fd: input.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: both descriptors are borrowed for this call and the initialized
        // poll array contains exactly the supplied number of entries.
        let result = unsafe {
            libc::poll(
                fds.as_mut_ptr(),
                fds.len() as libc::nfds_t,
                /*timeout*/ -1,
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if fds[1].revents != 0 {
            return Ok(false);
        }
        if fds[0].revents != 0 {
            return Ok(true);
        }
    }
}
