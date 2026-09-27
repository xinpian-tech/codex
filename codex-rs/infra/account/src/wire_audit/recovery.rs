use std::fs;
use std::fs::TryLockError;
use std::io;
use std::io::Write;
use std::path::Path;

use codex_infra_state::Journal;
use serde_json::Value;
use serde_json::json;

use super::AccountExchangeFinished;
use super::AccountExchangeIdentity;

/// Blocking recovery after the connection writer and all its pending journal
/// workers have exited. A live connection is skipped. Existing terminal events
/// retain their recorded outcome; owner exit never implies request success.
pub fn recover_account_exchange(directory: &Path) -> io::Result<Option<AccountExchangeFinished>> {
    if let Some(finished) = read_finished(directory)? {
        return Ok(Some(finished));
    }
    let lock = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("run.lock"))
    {
        Ok(lock) => lock,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    match lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(None),
        Err(TryLockError::Error(error)) => return Err(error),
    }
    if let Some(finished) = read_finished(directory)? {
        return Ok(Some(finished));
    }
    if !directory.join("io.journal").try_exists()? {
        return Ok(None);
    }
    let mut identity = None::<AccountExchangeIdentity>;
    let mut terminal = false;
    let mut journal = Journal::open(&directory.join("io.journal"), |record| {
        let event: Value = serde_json::from_slice(&record.payload)?;
        match event["kind"].as_str() {
            Some("opened") if identity.is_none() => {
                identity = Some(serde_json::from_value(event["identity"].clone())?);
            }
            Some("received" | "send_planned" | "sent") if identity.is_some() && !terminal => {}
            Some("finished") if identity.is_some() && !terminal => terminal = true,
            Some("owner_exit_recovered") if identity.is_some() => terminal = true,
            _ => return Err(io::Error::other("account exchange audit lifecycle differs")),
        }
        Ok(())
    })?;
    let Some(identity) = identity else {
        return Ok(None);
    };
    if directory.file_name() != Some(std::ffi::OsStr::new(&identity.connection_id.to_string())) {
        return Err(io::Error::other(
            "account exchange directory differs from identity",
        ));
    }
    journal.append(&serde_json::to_vec(&json!({
        "kind": "owner_exit_recovered", "terminal_previously_recorded": terminal,
    }))?)?;
    let finished = AccountExchangeFinished {
        identity,
        position: journal.position(),
    };
    publish_finished(directory, &finished)?;
    Ok(Some(finished))
}

fn read_finished(directory: &Path) -> io::Result<Option<AccountExchangeFinished>> {
    match fs::read(directory.join("finished.json")) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) fn publish_finished(
    directory: &Path,
    finished: &AccountExchangeFinished,
) -> io::Result<()> {
    let pending = directory.join("finished.pending");
    let mut file = fs::File::create(&pending)?;
    file.write_all(&serde_json::to_vec(finished)?)?;
    file.sync_all()?;
    fs::rename(pending, directory.join("finished.json"))?;
    #[cfg(unix)]
    fs::File::open(directory)?.sync_all()?;
    Ok(())
}
