use std::io;
use std::num::NonZeroUsize;

use codex_infra_protocol::AgentMessage;

use crate::BootstrapSelection;
use crate::LaunchIntent;

use super::TerminalInputState;
use super::TerminalMailbox;

impl TerminalMailbox {
    /// Reads bounded inbox pages while the independent input thread continues
    /// capturing frames. The watch cursor is sampled before scanning so an
    /// arrival during the scan cannot be lost between lookup and wait.
    pub async fn wait_bootstrap(
        &self,
        launch: LaunchIntent,
        limit: NonZeroUsize,
    ) -> io::Result<AgentMessage> {
        let mut state = self.subscribe();
        let mut cursor = 0;
        loop {
            let observed = state.borrow_and_update().clone();
            let binding = launch.clone();
            let selection = self
                .with_mailbox(move |mailbox| mailbox.select_bootstrap(&binding, cursor, limit))
                .await?;
            match selection {
                BootstrapSelection::Selected(message) => return Ok(*message),
                BootstrapSelection::Pending {
                    next_cursor: Some(next),
                } => cursor = next,
                BootstrapSelection::Pending { next_cursor: None } => {
                    if let TerminalInputState::Stopped { error } = observed {
                        return Err(io::Error::other(format!(
                            "terminal stopped before bootstrap: {error:?}"
                        )));
                    }
                    state.changed().await.map_err(|_| {
                        io::Error::other("terminal input worker ended before bootstrap")
                    })?;
                    cursor = 0;
                }
            }
        }
    }
}
