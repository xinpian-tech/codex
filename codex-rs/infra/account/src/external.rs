use std::future::Future;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::InferenceBinding;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_login::AuthDotJson;
use codex_login::CodexAuth;
use codex_login::ExternalAuth;
use codex_login::ExternalAuthFuture;
use codex_login::ExternalAuthRefreshContext;
use serde_json::Value;
use serde_json::json;

/// A credential whose snapshot and catalog binding have both been published.
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PublishedAccount {
    pub provider_id: String,
    pub account_id: String,
    pub credential_revision: CommitId,
    pub config_commit: CommitId,
    pub auth: AuthDotJson,
}

/// Supplies committed account revisions to one Agent. Implementations own
/// cross-Agent refresh serialization, credential persistence and config push.
/// Refresh first checks whether previous_revision has already been replaced;
/// repeated/canceled callers must not consume the same refresh token twice.
/// Neither method returns a new revision before its publication is confirmed.
pub trait AccountCredentialSource: Send + Sync {
    fn current(&self) -> impl Future<Output = io::Result<PublishedAccount>> + Send;

    fn refresh(
        &self,
        previous_revision: CommitId,
        context: ExternalAuthRefreshContext,
    ) -> impl Future<Output = io::Result<PublishedAccount>> + Send;
}

struct ObservedAccount {
    path: PathBuf,
    journal: Journal,
    current: Option<(PublishedAccount, CodexAuth)>,
}

/// Read-side handle for archiving the actual credential revisions adopted by
/// this Agent. The caller quiesces auth users before requesting a settled prefix.
#[derive(Clone)]
pub struct AccountObservation {
    gate: Arc<tokio::sync::Semaphore>,
    observed: Arc<Mutex<ObservedAccount>>,
}

impl AccountObservation {
    pub fn settled_snapshot(&self) -> io::Result<(PathBuf, JournalPosition)> {
        let _permit = Arc::clone(&self.gate).try_acquire_owned().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "account authentication is still active",
            )
        })?;
        let observed = self
            .observed
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok((observed.path.clone(), observed.journal.position()))
    }
}

/// Converts published login snapshots to external access-token auth. Refresh
/// tokens stay in the account source; embedded AuthManagers receive access-only
/// auth and delegate refresh back to that source.
pub struct PublishedAccountAuth<S> {
    binding: InferenceBinding,
    source: S,
    gate: Arc<tokio::sync::Semaphore>,
    observed: Arc<Mutex<ObservedAccount>>,
}

impl<S: AccountCredentialSource> PublishedAccountAuth<S> {
    pub fn observation(&self) -> AccountObservation {
        AccountObservation {
            gate: Arc::clone(&self.gate),
            observed: Arc::clone(&self.observed),
        }
    }

    pub fn binding(&self) -> &InferenceBinding {
        &self.binding
    }

    /// Opens the Agent-local observation journal before any manager uses auth.
    /// The caller creates its parent directory and runs this on a blocking worker.
    pub fn open(binding: InferenceBinding, source: S, journal: &Path) -> io::Result<Self> {
        let path = journal.to_path_buf();
        let expected = serde_json::to_value(&binding)?;
        let mut opened = false;
        let mut journal = Journal::open(journal, |record| {
            let event: Value = serde_json::from_slice(&record.payload)?;
            if event["event"] == "opened" {
                if event["binding"] != expected {
                    return Err(io::Error::other("external account journal binding differs"));
                }
                opened = true;
            }
            Ok(())
        })?;
        if !opened {
            journal.append(&serde_json::to_vec(&json!({
                "event": "opened", "binding": expected,
            }))?)?;
        }
        Ok(Self {
            binding,
            source,
            gate: Arc::new(tokio::sync::Semaphore::new(/*permits*/ 1)),
            observed: Arc::new(Mutex::new(ObservedAccount {
                path: path.canonicalize()?,
                journal,
                current: None,
            })),
        })
    }

    async fn adopt(
        &self,
        published: PublishedAccount,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> io::Result<CodexAuth> {
        if published.provider_id != self.binding.provider_id
            || published.account_id != self.binding.account_id
        {
            return Err(io::Error::other(
                "published account differs from Agent selection",
            ));
        }
        let observed = Arc::clone(&self.observed);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut observed = observed
                .lock()
                .map_err(|_| io::Error::other("account observation poisoned"))?;
            if let Some((current, auth)) = &observed.current {
                if current == &published {
                    return Ok(auth.clone());
                }
                if current.credential_revision == published.credential_revision
                    && current.auth != published.auth
                {
                    return Err(io::Error::other(
                        "credential contents changed within a revision",
                    ));
                }
            }
            let tokens = published
                .auth
                .tokens
                .as_ref()
                .ok_or_else(|| io::Error::other("published Codex login tokens missing"))?;
            let account_id = tokens
                .account_id
                .as_deref()
                .or(tokens.id_token.chatgpt_account_id.as_deref())
                .ok_or_else(|| io::Error::other("published ChatGPT account ID missing"))?;
            let plan = tokens.id_token.get_chatgpt_plan_type_raw();
            let auth = CodexAuth::from_external_chatgpt_tokens(
                &tokens.access_token,
                account_id,
                plan.as_deref(),
            )?;
            observed.journal.append(&serde_json::to_vec(&json!({
                "event": "revision_adopted",
                "provider_id": published.provider_id,
                "account_id": published.account_id,
                "credential_revision": published.credential_revision,
                "config_commit": published.config_commit,
                "auth": published.auth,
            }))?)?;
            observed.current = Some((published, auth.clone()));
            Ok(auth)
        })
        .await
        .map_err(io::Error::other)?
    }
}

impl<S: AccountCredentialSource> ExternalAuth for PublishedAccountAuth<S> {
    fn resolve(&self) -> ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(async {
            let permit = Arc::new(
                Arc::clone(&self.gate)
                    .acquire_owned()
                    .await
                    .map_err(io::Error::other)?,
            );
            self.adopt(self.source.current().await?, permit).await
        })
    }

    fn refresh(&self, context: ExternalAuthRefreshContext) -> ExternalAuthFuture<'_, CodexAuth> {
        Box::pin(async move {
            let permit = Arc::new(
                Arc::clone(&self.gate)
                    .acquire_owned()
                    .await
                    .map_err(io::Error::other)?,
            );
            let observed_revision = {
                let observed = self
                    .observed
                    .lock()
                    .map_err(|_| io::Error::other("account observation poisoned"))?;
                observed
                    .current
                    .as_ref()
                    .map(|(account, _)| account.credential_revision.clone())
            };
            let published = self.source.current().await?;
            let previous_revision = published.credential_revision.clone();
            let current = self.adopt(published, Arc::clone(&permit)).await?;
            if let Some(previous_account_id) = &context.previous_account_id
                && current.get_account_id().as_ref() != Some(previous_account_id)
            {
                return Err(io::Error::other(
                    "refresh account differs from previous request",
                ));
            }
            if observed_revision.is_some_and(|revision| revision != previous_revision) {
                return Ok(current);
            }
            self.adopt(
                self.source.refresh(previous_revision, context).await?,
                permit,
            )
            .await
        })
    }
}
