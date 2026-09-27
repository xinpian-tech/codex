use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_state::Journal;
use codex_login::AuthCredentialsStoreMode;
use codex_login::AuthDotJson;
use codex_login::AuthManager;
use codex_login::AuthRouteConfig;
use codex_login::CodexAuth;
use serde_json::Value;
use serde_json::json;

use crate::PublishedAccount;

/// Machine-owner paths and auth routing for one account. Every refresh of the
/// same credential revision uses the same directory beneath this spool.
#[derive(Clone)]
pub struct CodexRefreshConfig {
    pub directory: PathBuf,
    pub chatgpt_base_url: String,
    pub auth_route: AuthRouteConfig,
}

struct Prepared {
    journal: Journal,
    home: PathBuf,
    recovered: Option<AuthDotJson>,
}

/// Executes and persists a single revision's OAuth refresh using codex-login.
/// The machine account owner calls this only after confirming refresh ownership.
/// The returned auth is unpublished: the owner must commit/push it before
/// returning a PublishedAccount to Agents. Cancellation drops only the waiter.
pub async fn refresh_codex_account(
    config: CodexRefreshConfig,
    account: PublishedAccount,
) -> io::Result<AuthDotJson> {
    tokio::spawn(async move {
        let initial = account.clone();
        let directory = config.directory.clone();
        let prepared = tokio::task::spawn_blocking(move || prepare(&directory, &initial))
            .await
            .map_err(io::Error::other)??;
        let Prepared {
            journal,
            home,
            recovered,
        } = prepared;
        if let Some(auth) = recovered {
            return Ok(auth);
        }
        let manager = AuthManager::new(
            home.clone(),
            /*enable_codex_api_key_env*/ false,
            AuthCredentialsStoreMode::File,
            /*forced_chatgpt_workspace_id*/ None,
            Some(config.chatgpt_base_url),
            Default::default(),
            config.auth_route,
        )
        .await;
        let cached = manager.auth_cached();
        if !matches!(cached, Some(CodexAuth::Chatgpt(_)))
            || cached
                .as_ref()
                .and_then(|auth| auth.get_token_data().ok())
                .as_ref()
                != account.auth.tokens.as_ref()
        {
            return Err(io::Error::other(
                "refresh manager did not load the selected login snapshot",
            ));
        }
        let mut journal = tokio::task::spawn_blocking(move || {
            let mut journal = journal;
            journal.append(&serde_json::to_vec(&json!({"event": "refresh_requested"}))?)?;
            Ok::<_, io::Error>(journal)
        })
        .await
        .map_err(io::Error::other)??;
        let refreshed = manager.refresh_token_from_authority().await;
        tokio::task::spawn_blocking(move || {
            let stored = codex_login::load_auth_dot_json(
                &home,
                AuthCredentialsStoreMode::File,
                Default::default(),
            )?
            .ok_or_else(|| io::Error::other("refresh auth.json missing"))?;
            if stored != account.auth {
                validate_result(&account.auth, &stored)?;
                fs::File::open(home.join("auth.json"))?.sync_all()?;
                #[cfg(unix)]
                fs::File::open(&home)?.sync_all()?;
                journal.append(&serde_json::to_vec(&json!({
                    "event": "refreshed", "auth": stored,
                    "refresh_error": refreshed.err().map(|error| error.to_string()),
                }))?)?;
                return Ok(stored);
            }
            let message = match refreshed {
                Ok(()) => "refresh completed without a changed credential snapshot".to_owned(),
                Err(error) => error.to_string(),
            };
            journal.append(&serde_json::to_vec(
                &json!({"event": "failed", "error": message}),
            )?)?;
            Err(io::Error::other(message))
        })
        .await
        .map_err(io::Error::other)?
    })
    .await
    .map_err(io::Error::other)?
}

fn prepare(directory: &Path, account: &PublishedAccount) -> io::Result<Prepared> {
    let identity = serde_json::to_vec(&(&account.provider_id, &account.account_id))?;
    let account_directory = directory.join(blake3::hash(&identity).to_hex().as_str());
    let home = account_directory.join(account.credential_revision.to_string());
    fs::create_dir_all(&home)?;
    #[cfg(unix)]
    {
        fs::File::open(&account_directory)?.sync_all()?;
        fs::File::open(directory)?.sync_all()?;
    }
    let home = home.canonicalize()?;
    let binding = json!({
        "provider_id": account.provider_id, "account_id": account.account_id,
        "credential_revision": account.credential_revision, "auth": account.auth,
    });
    let mut opened = false;
    let mut requested = false;
    let mut recovered = None;
    let mut failed = None;
    let mut journal = Journal::open(&home.join("refresh.journal"), |record| {
        let event: Value = serde_json::from_slice(&record.payload)?;
        match event["event"].as_str() {
            Some("opened") if event["binding"] == binding => opened = true,
            Some("refresh_requested") if opened => requested = true,
            Some("refreshed") if requested => {
                recovered = Some(serde_json::from_value(event["auth"].clone())?);
            }
            Some("failed") if requested => failed = event["error"].as_str().map(str::to_owned),
            _ => {
                return Err(io::Error::other(
                    "refresh journal binding or lifecycle differs",
                ));
            }
        }
        Ok(())
    })?;
    if !opened {
        journal.append(&serde_json::to_vec(
            &json!({"event": "opened", "binding": binding}),
        )?)?;
    }
    if let Some(auth) = &recovered {
        validate_result(&account.auth, auth)?;
    } else if requested {
        let stored = codex_login::load_auth_dot_json(
            &home,
            AuthCredentialsStoreMode::File,
            Default::default(),
        )?;
        if let Some(stored) = stored
            && stored != account.auth
        {
            validate_result(&account.auth, &stored)?;
            fs::File::open(home.join("auth.json"))?.sync_all()?;
            journal.append(&serde_json::to_vec(
                &json!({"event": "refreshed", "auth": stored, "recovered": true}),
            )?)?;
            recovered = Some(stored);
        } else {
            return Err(io::Error::other(failed.unwrap_or_else(|| {
                "refresh outcome is unknown; the consumed revision requires reconciliation"
                    .to_owned()
            })));
        }
    } else {
        let tokens = account
            .auth
            .tokens
            .as_ref()
            .ok_or_else(|| io::Error::other("refresh tokens missing"))?;
        if tokens.refresh_token.is_empty() {
            return Err(io::Error::other("selected revision has no refresh token"));
        }
        codex_login::save_auth(
            &home,
            &account.auth,
            AuthCredentialsStoreMode::File,
            Default::default(),
        )?;
        fs::File::open(home.join("auth.json"))?.sync_all()?;
        #[cfg(unix)]
        fs::File::open(&home)?.sync_all()?;
    }
    Ok(Prepared {
        journal,
        home,
        recovered,
    })
}

fn validate_result(before: &AuthDotJson, after: &AuthDotJson) -> io::Result<()> {
    let before = before
        .tokens
        .as_ref()
        .ok_or_else(|| io::Error::other("original login tokens missing"))?;
    let after = after
        .tokens
        .as_ref()
        .ok_or_else(|| io::Error::other("refreshed login tokens missing"))?;
    let before_account = before
        .account_id
        .as_ref()
        .or(before.id_token.chatgpt_account_id.as_ref());
    let after_account = after
        .account_id
        .as_ref()
        .or(after.id_token.chatgpt_account_id.as_ref());
    if before_account.is_none() || before_account != after_account || after.access_token.is_empty()
    {
        return Err(io::Error::other(
            "refreshed login does not match the selected account",
        ));
    }
    Ok(())
}
