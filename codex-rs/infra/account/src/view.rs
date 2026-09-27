use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_protocol::ConfigGeneration;
use codex_infra_protocol::InferenceBinding;
use codex_infra_provider::AccountAuthentication;
use codex_infra_provider::ProviderProtocol;
use codex_infra_provider::ResolvedAccount;
use codex_infra_state::Journal;
use codex_login::AuthCredentialsStoreMode;
use codex_login::AuthDotJson;
use serde_json::Value;
use serde_json::json;

/// Owns one Agent's local native-login view. Retain this handle while the host
/// uses the home; its journal serializes materialization and later reopening.
pub struct CodexAccountView {
    home: PathBuf,
    binding: InferenceBinding,
    _journal: Journal,
}

impl CodexAccountView {
    /// Materializes the selected Device Code snapshot through Codex's existing
    /// file auth store. Reopening an initialized view preserves its auth file.
    /// This blocking operation belongs before AuthManager construction.
    pub fn prepare(generation: &ConfigGeneration, home: &Path) -> io::Result<Self> {
        let resolved = ResolvedAccount::read_generation(generation)?;
        if resolved.provider.protocol != ProviderProtocol::Responses {
            return Err(io::Error::other("native Codex account requires Responses"));
        }
        let auth: AuthDotJson = match resolved.account.authentication {
            AccountAuthentication::CodexLogin { auth } => serde_json::from_value(auth)?,
            AccountAuthentication::BearerToken { .. }
            | AccountAuthentication::HeaderToken { .. } => {
                return Err(io::Error::other(
                    "account does not contain Codex login data",
                ));
            }
        };
        fs::create_dir_all(home)?;
        let home = home.canonicalize()?;
        let expected = serde_json::to_value(&resolved.binding)?;
        let mut opened = false;
        let mut prepared = false;
        let mut journal = Journal::open(&home.join("infra-account-view.journal"), |record| {
            let event: Value = serde_json::from_slice(&record.payload)?;
            match event["event"].as_str() {
                Some("opened") if event["binding"] == expected => opened = true,
                Some("prepared") if opened => prepared = true,
                _ => {
                    return Err(io::Error::other(
                        "account view binding or lifecycle differs",
                    ));
                }
            }
            Ok(())
        })?;
        if !opened {
            journal.append(&serde_json::to_vec(&json!({
                "event": "opened", "binding": expected,
                "generation": generation.config_store_path,
            }))?)?;
        }
        if !prepared {
            codex_login::save_auth(
                &home,
                &auth,
                AuthCredentialsStoreMode::File,
                Default::default(),
            )?;
            fs::File::open(home.join("auth.json"))?.sync_all()?;
            #[cfg(unix)]
            fs::File::open(&home)?.sync_all()?;
            journal.append(&serde_json::to_vec(&json!({"event": "prepared"}))?)?;
        }
        let present = codex_login::load_auth_dot_json(
            &home,
            AuthCredentialsStoreMode::File,
            Default::default(),
        )?;
        if present.is_none() {
            return Err(io::Error::other("prepared account auth.json is missing"));
        }
        if present.as_ref() != Some(&auth) {
            return Err(io::Error::other(
                "account view credentials differ from the selected generation revision",
            ));
        }
        Ok(Self {
            home,
            binding: resolved.binding,
            _journal: journal,
        })
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn binding(&self) -> &InferenceBinding {
        &self.binding
    }
}
