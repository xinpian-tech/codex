use std::fs;
use std::io;
use std::path::PathBuf;

use codex_infra_protocol::AgentId;
use codex_infra_state::JournalPosition;

use super::InputScheduler;
use crate::PaneInputJournal;

impl InputScheduler {
    /// Acquires an idle Agent's actual input writer before sampling. Old streams
    /// not yet used by this scheduler are reopened through normal journal replay
    /// and fsync, so recovered positions are writer acknowledgments as well.
    /// Active commands retain their writer until finish_next hands it back.
    pub(crate) fn archive_source(
        &mut self,
        agent_id: AgentId,
    ) -> io::Result<Option<(PathBuf, JournalPosition)>> {
        if self.busy.contains(&agent_id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "pane input writer is executing a command",
            ));
        }
        let path = self.directory.join(format!("{agent_id}.journal"));
        if let std::collections::btree_map::Entry::Vacant(entry) = self.journals.entry(agent_id) {
            match fs::metadata(&path) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            }
            entry.insert(PaneInputJournal::open(&path)?);
        }
        let journal = self
            .journals
            .get(&agent_id)
            .ok_or_else(|| io::Error::other("pane input writer missing after acquisition"))?;
        Ok(Some((path, journal.position())))
    }
}
