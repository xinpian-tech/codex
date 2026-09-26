use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;

/// A producer-published durable prefix, atomically replaced after journal fsync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderAttemptProgress {
    pub attempt_id: MessageId,
    pub positions: BTreeMap<String, JournalPosition>,
}

impl ProviderAttemptProgress {
    pub(super) fn publish(&self, directory: &Path) -> io::Result<()> {
        let temporary = directory.join("progress.tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(&serde_json::to_vec(self)?)?;
        file.sync_all()?;
        drop(file);
        fs::rename(temporary, directory.join("progress.json"))?;
        #[cfg(unix)]
        File::open(directory)?.sync_all()?;
        Ok(())
    }

    pub fn read(directory: &Path) -> io::Result<Option<Self>> {
        match fs::read(directory.join("progress.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(io::Error::other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}
