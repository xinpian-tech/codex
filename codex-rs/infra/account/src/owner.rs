use std::fs;
use std::io;
use std::path::PathBuf;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_login::ExternalAuthRefreshContext;
use serde_json::Value;
use serde_json::json;

use crate::AccountCredentialSource;
use crate::CodexRefreshConfig;
use crate::GitAccounts;
use crate::PublishedAccount;
use crate::refresh_codex_account;

/// Committed account assignment. Its revision is the last commit changing the
/// corresponding accounts/owners/<account-key>.json file, not the config tip.
/// Handover must drain the previous owner before publishing a new assignment.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountOwnerAssignment {
    pub provider_id: String,
    pub account_id: String,
    pub machine_id: MachineId,
}

impl AccountOwnerAssignment {
    pub fn path(&self) -> io::Result<String> {
        let identity = serde_json::to_vec(&(&self.provider_id, &self.account_id))?;
        Ok(format!("accounts/owners/{}.json", blake3::hash(&identity)))
    }
}

/// Machine-side implementation used by AccountService. All instances serving
/// this account on the machine use the same durable directory.
#[derive(Clone)]
pub struct GitAccountOwner {
    pub git: GitAccounts,
    pub assignment: AccountOwnerAssignment,
    pub owner_revision: CommitId,
    pub directory: PathBuf,
    pub refresh: CodexRefreshConfig,
}

impl GitAccountOwner {
    async fn read_current(&self) -> io::Result<PublishedAccount> {
        let owner = self.clone();
        tokio::task::spawn_blocking(move || {
            let account = owner.git.read_published_codex(
                &owner.assignment.provider_id,
                &owner.assignment.account_id,
            )?;
            owner.git.confirm_owner(
                &account.config_commit,
                &owner.assignment,
                &owner.owner_revision,
            )?;
            Ok(account)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn refresh_owned(
        self,
        previous_revision: CommitId,
        context: ExternalAuthRefreshContext,
    ) -> io::Result<PublishedAccount> {
        let directory = self.directory.clone();
        let assignment = self.assignment.clone();
        let revision = previous_revision.clone();
        let (journal, operation) = tokio::task::spawn_blocking(move || {
            let identity = serde_json::to_vec(&(&assignment.provider_id, &assignment.account_id))?;
            let directory = directory.join(blake3::hash(&identity).to_hex().as_str());
            fs::create_dir_all(&directory)?;
            let expected = json!({"assignment": assignment, "credential_revision": revision});
            let mut operation = None;
            let mut journal =
                Journal::open(&directory.join(format!("{revision}.journal")), |record| {
                    let event: Value = serde_json::from_slice(&record.payload)?;
                    if event["event"] == "opened" {
                        if event["binding"] != expected {
                            return Err(io::Error::other(
                                "refresh flow belongs to a different account",
                            ));
                        }
                        operation = Some(serde_json::from_value(event["operation_id"].clone())?);
                    }
                    Ok(())
                })?;
            let operation = match operation {
                Some(operation) => operation,
                None => {
                    let operation = MessageId::new();
                    journal.append(&serde_json::to_vec(&json!({
                        "event": "opened", "binding": expected, "operation_id": operation,
                    }))?)?;
                    operation
                }
            };
            Ok::<_, io::Error>((journal, operation))
        })
        .await
        .map_err(io::Error::other)??;
        // The journal remains owned through OAuth and publication, including
        // when the original TCP requester has stopped waiting.
        let current = self.read_current().await?;
        if let Some(expected) = context.previous_account_id {
            let actual = current.auth.tokens.as_ref().and_then(|tokens| {
                tokens
                    .account_id
                    .as_ref()
                    .or(tokens.id_token.chatgpt_account_id.as_ref())
            });
            if actual != Some(&expected) {
                return Err(io::Error::other(
                    "refresh request belongs to a different ChatGPT account",
                ));
            }
        }
        if current.credential_revision != previous_revision {
            return Ok(current);
        }
        let refreshed = refresh_codex_account(self.refresh.clone(), current).await?;
        let latest = self.read_current().await?;
        if latest.credential_revision != previous_revision {
            crate::refresh::validate_result(&refreshed, &latest.auth)?;
            return Ok(latest);
        }
        let git = self.git.clone();
        let auth = refreshed.clone();
        tokio::task::spawn_blocking(move || {
            let mut journal = journal;
            journal.append(&serde_json::to_vec(&json!({
                "event": "publication_requested", "operation_id": operation,
                "previous": latest, "auth": auth,
            }))?)?;
            let result = git.resume_refresh_publication(operation, &latest, auth);
            match result {
                Ok(commit) => {
                    journal.append(&serde_json::to_vec(
                        &json!({"event": "published", "config_commit": commit}),
                    )?)?;
                    Ok(())
                }
                Err(error) => {
                    journal.append(&serde_json::to_vec(
                        &json!({"event": "publication_failed", "error": error.to_string()}),
                    )?)?;
                    Err(error)
                }
            }
        })
        .await
        .map_err(io::Error::other)??;
        let published = self.read_current().await?;
        if published.credential_revision == previous_revision {
            return Err(io::Error::other(
                "remote account still references the consumed revision",
            ));
        }
        crate::refresh::validate_result(&refreshed, &published.auth)?;
        Ok(published)
    }
}

impl AccountCredentialSource for GitAccountOwner {
    async fn current(&self) -> io::Result<PublishedAccount> {
        self.read_current().await
    }

    async fn refresh(
        &self,
        previous_revision: CommitId,
        context: ExternalAuthRefreshContext,
    ) -> io::Result<PublishedAccount> {
        let owner = self.clone();
        tokio::spawn(owner.refresh_owned(previous_revision, context))
            .await
            .map_err(io::Error::other)?
    }
}
