use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::time::Duration;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::ConfigGeneration;
use codex_infra_protocol::InferenceBinding;
use reqwest::Url;
use reqwest::header::AUTHORIZATION;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderName;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use serde::Serialize;

use crate::ChatFrontendConfig;
use crate::ProviderAuditConfig;

/// Provider entries in the generation's providers/catalog.json, keyed by ID.
#[derive(Clone, Serialize, Deserialize)]
pub struct ProviderCatalog(pub BTreeMap<String, ProviderDefinition>);

/// Account entries in accounts/catalog.json, keyed first by provider, then account.
#[derive(Clone, Serialize, Deserialize)]
pub struct AccountCatalog(pub BTreeMap<String, BTreeMap<String, AccountDefinition>>);

/// Endpoint and transport are explicit Team State choices. The endpoint is the
/// complete upstream request URL, not a base URL with an inferred suffix.
#[derive(Clone, Serialize, Deserialize)]
pub struct ProviderDefinition {
    pub protocol: ProviderProtocol,
    pub endpoint: String,
    pub headers: BTreeMap<String, String>,
    pub models: Vec<String>,
    pub limits: ProviderClientLimits,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderProtocol {
    Responses,
    ChatCompletions,
}

/// Per-Agent transport limits; shared account admission is managed separately.
#[derive(Clone, Serialize, Deserialize)]
pub struct ProviderClientLimits {
    pub request_bytes: NonZeroUsize,
    pub response_bytes: NonZeroUsize,
    pub concurrent_requests: NonZeroUsize,
    pub connect_timeout_ms: NonZeroU64,
    pub request_timeout_ms: NonZeroU64,
}

/// The credential revision identifies the committed token snapshot used by a
/// launch. Account headers override provider headers before auth is applied.
#[derive(Clone, Serialize, Deserialize)]
pub struct AccountDefinition {
    pub credential_revision: CommitId,
    pub authentication: AccountAuthentication,
    pub headers: BTreeMap<String, String>,
}

/// External login tools import their resulting token using bearer or header
/// authentication. Codex login retains the existing auth.json representation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AccountAuthentication {
    BearerToken { token: String },
    HeaderToken { name: String, value: String },
    CodexLogin { auth: serde_json::Value },
}

/// The explicit account selection shared by native Responses and Chat adapters.
pub struct ResolvedAccount {
    pub binding: InferenceBinding,
    pub provider: ProviderDefinition,
    pub account: AccountDefinition,
}

impl ResolvedAccount {
    pub fn read_generation(generation: &ConfigGeneration) -> io::Result<Self> {
        let mut providers: ProviderCatalog = serde_json::from_slice(&fs::read(
            generation.config_store_path.join("providers/catalog.json"),
        )?)?;
        let mut accounts: AccountCatalog = serde_json::from_slice(&fs::read(
            generation.config_store_path.join("accounts/catalog.json"),
        )?)?;
        let binding = &generation.inference;
        let provider = providers
            .0
            .remove(&binding.provider_id)
            .ok_or_else(|| io::Error::other("generation provider is absent from catalog"))?;
        let account = accounts
            .0
            .get_mut(&binding.provider_id)
            .and_then(|accounts| accounts.remove(&binding.account_id))
            .ok_or_else(|| io::Error::other("generation account is absent from catalog"))?;
        if !provider.models.contains(&binding.model_id) {
            return Err(io::Error::other(
                "generation model is absent from provider models",
            ));
        }
        if account.credential_revision != binding.credential_revision {
            return Err(io::Error::other(
                "generation credential revision differs from account snapshot",
            ));
        }
        Ok(Self {
            binding: binding.clone(),
            provider,
            account,
        })
    }
}

impl ChatFrontendConfig {
    /// Reads the immutable account/provider catalogs selected by this generation.
    /// Call from a blocking worker when assembling an async Agent host.
    pub fn read_generation(
        generation: &ConfigGeneration,
        audit: ProviderAuditConfig,
    ) -> io::Result<Self> {
        let ResolvedAccount {
            binding,
            provider,
            account,
        } = ResolvedAccount::read_generation(generation)?;
        if provider.protocol != ProviderProtocol::ChatCompletions {
            return Err(io::Error::other(
                "generation provider does not use Chat Completions",
            ));
        }
        let endpoint = Url::parse(&provider.endpoint).map_err(io::Error::other)?;
        let mut headers = HeaderMap::new();
        for (name, value) in provider.headers.iter().chain(&account.headers) {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).map_err(io::Error::other)?,
                HeaderValue::from_str(value).map_err(io::Error::other)?,
            );
        }
        match &account.authentication {
            AccountAuthentication::BearerToken { token } => {
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&format!("Bearer {token}")).map_err(io::Error::other)?,
                );
            }
            AccountAuthentication::HeaderToken { name, value } => {
                headers.insert(
                    HeaderName::from_bytes(name.as_bytes()).map_err(io::Error::other)?,
                    HeaderValue::from_str(value).map_err(io::Error::other)?,
                );
            }
            AccountAuthentication::CodexLogin { .. } => {
                return Err(io::Error::other(
                    "Codex login uses the native Responses account path",
                ));
            }
        }
        Ok(Self {
            binding,
            audit,
            endpoint,
            headers,
            request_bytes: provider.limits.request_bytes,
            response_bytes: provider.limits.response_bytes,
            concurrent_requests: provider.limits.concurrent_requests,
            connect_timeout: Duration::from_millis(provider.limits.connect_timeout_ms.get()),
            request_timeout: Duration::from_millis(provider.limits.request_timeout_ms.get()),
        })
    }
}
