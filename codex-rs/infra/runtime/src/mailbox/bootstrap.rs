use std::io;
use std::io::Write;
use std::num::NonZeroUsize;

use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::MessageKind;
use codex_infra_protocol::TaskSpec;
use serde::Deserialize;
use serde::Serialize;

use crate::LaunchIntent;

use super::HostMailbox;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BootstrapBinding {
    launch: LaunchIntent,
    message_id: MessageId,
}

/// A bounded inbox scan, or the launch's durable original bootstrap selection.
pub enum BootstrapSelection {
    Selected(Box<AgentMessage>),
    Pending { next_cursor: Option<u64> },
}

impl<W: Write> HostMailbox<W> {
    /// Selects only from the receiving Agent's durable tmux inbox. Reopening
    /// returns the same message even after it has been presented or processed.
    /// Selection does not acknowledge presentation to the model.
    pub fn select_bootstrap(
        &mut self,
        launch: &LaunchIntent,
        first_sequence: u64,
        limit: NonZeroUsize,
    ) -> io::Result<BootstrapSelection> {
        if self.root_session_id != launch.workspace.root_session_id
            || self.agent_id != launch.workspace.agent_id
        {
            return Err(io::Error::other(
                "bootstrap launch differs from mailbox identity",
            ));
        }
        if let Some(binding) = &self.bootstrap_binding {
            if binding.launch != *launch {
                return Err(io::Error::other("mailbox bootstrap launch changed"));
            }
            let entry = self.inbox.get(binding.message_id).ok_or_else(|| {
                io::Error::other("selected bootstrap is absent from durable inbox")
            })?;
            return Ok(BootstrapSelection::Selected(Box::new(
                entry.message.clone(),
            )));
        }
        let mut count = 0;
        let mut next = None;
        let mut selected = None;
        for entry in self.inbox.pending_from(first_sequence, limit) {
            count += 1;
            next = Some(
                entry
                    .accepted_sequence
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("bootstrap scan sequence exhausted"))?,
            );
            let message = &entry.message;
            if message.kind != MessageKind::Bootstrap
                || message.task_id != launch.task_id
                || message.to.machine_id != launch.machine_id
                || message.to.role != launch.role
            {
                continue;
            }
            let task: TaskSpec = serde_json::from_str(&message.body)?;
            if task.task_id != launch.task_id
                || task.assigned_agent != launch.workspace.agent_id
                || task.target_machine != launch.machine_id
            {
                return Err(io::Error::other("bootstrap Task differs from launch"));
            }
            selected = Some(message.clone());
            break;
        }
        match selected {
            Some(message) => {
                let binding = BootstrapBinding {
                    launch: launch.clone(),
                    message_id: message.message_id,
                };
                self.bootstrap.append(&serde_json::to_vec(&binding)?)?;
                self.bootstrap_binding = Some(binding);
                Ok(BootstrapSelection::Selected(Box::new(message)))
            }
            None => Ok(BootstrapSelection::Pending {
                next_cursor: if count == limit.get() { next } else { None },
            }),
        }
    }
}
