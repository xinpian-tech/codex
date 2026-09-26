use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;

use codex_infra_protocol::MessageId;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
struct Progress {
    attachment_id: MessageId,
    stream: String,
    position: JournalPosition,
}

/// Each producer replaces its own small position file only after the journal
/// append has acknowledged fsync. A reader observes an old or new whole prefix;
/// it never derives durability from a concurrently growing file's length.
pub(super) fn publish(
    directory: &Path,
    stream: &str,
    attachment_id: MessageId,
    position: JournalPosition,
) -> io::Result<()> {
    let temporary = directory.join(format!("{stream}.position.tmp"));
    let mut file = File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(&Progress {
        attachment_id,
        stream: stream.to_owned(),
        position,
    })?)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, directory.join(format!("{stream}.position.json")))?;
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    Ok(())
}

pub(super) fn read(
    directory: &Path,
    stream: &str,
    attachment_id: MessageId,
) -> io::Result<Option<JournalPosition>> {
    let bytes = match fs::read(directory.join(format!("{stream}.position.json"))) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let progress: Progress = serde_json::from_slice(&bytes)?;
    if progress.attachment_id != attachment_id || progress.stream != stream {
        return Err(io::Error::other("collector progress identity mismatch"));
    }
    Ok(Some(progress.position))
}
