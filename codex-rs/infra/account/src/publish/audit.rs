use std::fs;
use std::io;
use std::path::PathBuf;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use serde_json::Value;
use serde_json::json;

use super::GitAccounts;

pub(super) struct PublicationAudit(Journal);

pub(super) struct RecoveryState {
    pub details: Value,
    pub before: Option<String>,
    pub target: Option<String>,
    pub completed: bool,
}

impl PublicationAudit {
    pub(super) fn resume(
        accounts: &GitAccounts,
        operation: MessageId,
    ) -> io::Result<(Self, RecoveryState)> {
        let path = PathBuf::from(accounts.run(
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                &format!("infra-account-publications/{operation}.journal"),
            ],
            &[],
        )?);
        fs::metadata(&path)?;
        let mut opened = None;
        let mut state = RecoveryState {
            details: Value::Null,
            before: None,
            target: None,
            completed: false,
        };
        let journal = Journal::open(&path, |record| {
            let event: Value = serde_json::from_slice(&record.payload)?;
            match event["event"].as_str() {
                Some("opened") => opened = Some(event.clone()),
                Some("base_selected") => {
                    state.before = event["commit"].as_str().map(str::to_owned);
                }
                Some("ref_update_requested") => {
                    state.before = event["before"].as_str().map(str::to_owned);
                    state.target = event["config_commit"].as_str().map(str::to_owned);
                }
                Some("push_requested") => {
                    state.target = event["config_commit"].as_str().map(str::to_owned);
                }
                Some("completed") => {
                    state.target = event["config_commit"].as_str().map(str::to_owned);
                    state.completed = true;
                }
                _ => {}
            }
            Ok(())
        })?;
        let opened = opened.ok_or_else(|| io::Error::other("publication identity missing"))?;
        if opened["operation_id"] != json!(operation)
            || opened["repository"] != json!(accounts.repository.canonicalize()?)
            || opened["remote"] != json!(accounts.remote)
            || opened["config_ref"] != json!(accounts.config_ref)
        {
            return Err(io::Error::other(
                "publication belongs to a different binding",
            ));
        }
        state.details = opened["details"].clone();
        Ok((Self(journal), state))
    }

    pub(super) fn open(
        accounts: &GitAccounts,
        operation: MessageId,
        details: Value,
    ) -> io::Result<Self> {
        let directory = PathBuf::from(accounts.run(
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                "infra-account-publications",
            ],
            &[],
        )?);
        fs::create_dir_all(&directory)?;
        #[cfg(unix)]
        if let Some(parent) = directory.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        let journal = Journal::open(&directory.join(format!("{operation}.journal")), |_| Ok(()))?;
        let mut audit = Self(journal);
        audit.record(json!({
            "event": "opened",
            "operation_id": operation,
            "repository": accounts.repository.canonicalize()?,
            "git": accounts.git,
            "remote": accounts.remote,
            "config_ref": accounts.config_ref,
            "details": details,
        }))?;
        Ok(audit)
    }

    pub(super) fn record(&mut self, value: Value) -> io::Result<()> {
        self.0.append(&serde_json::to_vec(&value)?)?;
        Ok(())
    }

    pub(super) fn finish(&mut self, result: &io::Result<CommitId>) -> io::Result<()> {
        self.record(match result {
            Ok(commit) => json!({"event": "completed", "config_commit": commit}),
            Err(error) => json!({"event": "failed", "error": error.to_string()}),
        })
    }
}
