use std::io;
use std::os::fd::AsFd;
use std::os::fd::OwnedFd;

use rustix::termios::OptionalActions;
use rustix::termios::SpecialCodeIndex;
use rustix::termios::Termios;
use rustix::termios::tcgetattr;
use rustix::termios::tcsetattr;

/// Keeps the Agent's tmux slave PTY in raw mode for framed stdin/stdout.
/// The host retains this guard until its final output has been flushed, and
/// advertises readiness only after `enter` and mailbox recovery complete.
pub struct AgentTerminal {
    terminal: OwnedFd,
    original: Termios,
    active: bool,
}

impl AgentTerminal {
    pub fn enter(input: impl AsFd) -> io::Result<Self> {
        let terminal = input.as_fd().try_clone_to_owned()?;
        let original = tcgetattr(&terminal)?;
        let mut raw = original.clone();
        raw.make_raw();
        raw.special_codes[SpecialCodeIndex::VMIN] = 1;
        raw.special_codes[SpecialCodeIndex::VTIME] = 0;
        tcsetattr(&terminal, OptionalActions::Now, &raw)?;
        Ok(Self {
            terminal,
            original,
            active: true,
        })
    }

    /// Explicit restoration lets the finalizer record an OS error. Drop also
    /// restores the terminal when unwinding the host's normal lifecycle.
    pub fn restore(&mut self) -> io::Result<()> {
        if self.active {
            tcsetattr(&self.terminal, OptionalActions::Now, &self.original)?;
            self.active = false;
        }
        Ok(())
    }
}

impl Drop for AgentTerminal {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}
