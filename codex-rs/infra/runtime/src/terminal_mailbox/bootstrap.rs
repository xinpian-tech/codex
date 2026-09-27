use std::io;
use std::num::NonZeroUsize;

use codex_infra_protocol::AgentMessage;
use codex_infra_state::InboxEntry;

use crate::BootstrapSelection;
use crate::LaunchIntent;

use super::TerminalInputState;
use super::TerminalMailbox;

impl TerminalMailbox {
    /// Restores the original acceptance sequence and presentation binding as
    /// well as the selected envelope, including after it has been processed.
    pub async fn wait_bootstrap_entry(
        &self,
        launch: LaunchIntent,
        limit: NonZeroUsize,
    ) -> io::Result<InboxEntry> {
        let selected = self.wait_bootstrap(launch, limit).await?;
        self.with_mailbox(move |mailbox| {
            let entry = mailbox
                .input(selected.message_id)
                .ok_or_else(|| io::Error::other("selected bootstrap disappeared"))?;
            if entry.message != selected {
                return Err(io::Error::other("selected bootstrap changed"));
            }
            Ok(entry.clone())
        })
        .await
    }

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
