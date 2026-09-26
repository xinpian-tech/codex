use std::fs;
use std::io;
use std::path::PathBuf;

use codex_infra_protocol::AgentId;
use codex_infra_state::JournalPosition;

use super::InputScheduler;
use crate::PaneInputJournal;

impl InputScheduler {
    /// Samples an idle Agent's actual writer. Historical streams are reopened by
    /// bounded blocking workers; WouldBlock leaves that recovery owned by this
    /// scheduler, even if the archive request's caller stops waiting.
    pub(crate) fn archive_source(
        &mut self,
        agent_id: AgentId,
    ) -> io::Result<Option<(PathBuf, JournalPosition)>> {
        self.finish_restores()?;
        if self.busy.contains(&agent_id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "pane input writer is executing or recovering",
            ));
        }
        let path = self.directory.join(format!("{agent_id}.journal"));
        if let Some(outcome) = self.restore_outcomes.remove(&agent_id) {
            outcome?;
            return Ok(None);
        }
        if let Some(journal) = self.journals.get(&agent_id) {
            return Ok(Some((path, journal.position())));
        }
        if self.pending.len() + self.restoring.len() >= self.limit.get() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "pane input recovery capacity is busy",
            ));
        }
        let task = self.restores.spawn_blocking(move || {
            match fs::metadata(&path) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            }
            PaneInputJournal::open(&path).map(Some)
        });
        self.restoring.insert(task.id(), agent_id);
        self.busy.insert(agent_id);
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "pane input recovery started",
        ))
    }

    pub(super) fn finish_restores(&mut self) -> io::Result<()> {
        while let Some(completed) = self.restores.try_join_next_with_id() {
            let (task_id, result) = match completed {
                Ok(completed) => completed,
                Err(error) => (error.id(), Err(io::Error::other(error))),
            };
            let agent_id = self
                .restoring
                .remove(&task_id)
                .ok_or_else(|| io::Error::other("pane input recovery binding missing"))?;
            self.busy.remove(&agent_id);
            match result {
                Ok(Some(journal)) => {
                    self.journals.insert(agent_id, journal);
                }
                Ok(None) => {
                    self.restore_outcomes.insert(agent_id, Ok(()));
                }
                Err(error) => {
                    self.restore_outcomes.insert(agent_id, Err(error));
                }
            }
        }
        Ok(())
    }
}
