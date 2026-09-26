use std::future::Future;
use std::io;
use std::path::Path;

use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::ProviderAttemptIdentity;
use crate::TranslationError;

/// Latest durable report for an attempt, joined to the original account and
/// credential revision. Aggregators replace this key's earlier revision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderAttemptUsage {
    pub identity: ProviderAttemptIdentity,
    pub revision: u64,
    pub usage: ProviderUsage,
}

impl ProviderAttemptUsage {
    pub fn read(directory: &Path) -> io::Result<Option<Self>> {
        let Some(identity) = ProviderAttemptIdentity::read(directory)? else {
            return Ok(None);
        };
        let mut reader = JournalReader::open(
            &directory.join("lifecycle.journal"),
            JournalPosition::default(),
        )?;
        let mut latest = None::<Self>;
        while let Some(record) = reader.next_record()? {
            let event: Value = serde_json::from_slice(&record.payload)?;
            if event["event"] != "usage_observed" {
                continue;
            }
            if event["attempt_id"] != serde_json::to_value(identity.attempt_id)? {
                return Err(io::Error::other("provider usage attempt mismatch"));
            }
            let revision = event["revision"]
                .as_u64()
                .ok_or_else(|| io::Error::other("provider usage revision missing"))?;
            let expected = latest
                .as_ref()
                .map_or(0, |previous| previous.revision)
                .checked_add(1)
                .ok_or_else(|| io::Error::other("provider usage revision exhausted"))?;
            if revision != expected {
                return Err(io::Error::other("provider usage revision sequence"));
            }
            let usage: ProviderUsage = serde_json::from_value(event["usage"].clone())?;
            if latest.as_ref().is_some_and(|previous| {
                previous.usage.provider_response_id != usage.provider_response_id
            }) {
                return Err(io::Error::other("provider usage response identity changed"));
            }
            latest = Some(Self {
                identity: identity.clone(),
                revision,
                usage,
            });
        }
        Ok(latest)
    }
}

/// Cumulative usage reported for one provider response. Revisions replace the
/// earlier totals for that response; they are not independent charges to sum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderUsage {
    pub provider_response_id: String,
    pub model: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cached_input_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub raw: Value,
}

impl ProviderUsage {
    pub(crate) fn from_chunk(chunk: &Value) -> Result<Option<Self>, TranslationError> {
        let Some(raw) = chunk.get("usage").filter(|usage| !usage.is_null()) else {
            return Ok(None);
        };
        let count = |name: &str| {
            raw[name]
                .as_u64()
                .ok_or_else(|| TranslationError::Invalid(format!("usage.{name}")))
        };
        let provider_response_id = chunk["id"]
            .as_str()
            .ok_or_else(|| TranslationError::Invalid("usage response ID".to_owned()))?
            .to_owned();
        let cached = raw
            .pointer("/prompt_tokens_details/cached_tokens")
            .or_else(|| raw.get("prompt_cache_hit_tokens"));
        let reasoning = raw.pointer("/completion_tokens_details/reasoning_tokens");
        let optional =
            |name: &str, value: Option<&Value>| -> Result<Option<u64>, TranslationError> {
                value
                    .filter(|value| !value.is_null())
                    .map(|value| {
                        value
                            .as_u64()
                            .ok_or_else(|| TranslationError::Invalid(format!("usage.{name}")))
                    })
                    .transpose()
            };
        Ok(Some(Self {
            provider_response_id,
            model: chunk["model"].as_str().map(str::to_owned),
            input_tokens: count("prompt_tokens")?,
            output_tokens: count("completion_tokens")?,
            total_tokens: count("total_tokens")?,
            cached_input_tokens: optional("cached_tokens", cached)?,
            reasoning_tokens: optional("reasoning_tokens", reasoning)?,
            raw: raw.clone(),
        }))
    }
}

/// Persists usage at receipt time, before completion or downstream delivery.
/// Implementations associate observations with the immutable request binding
/// and deduplicate/revise totals by provider response ID within that attempt.
pub trait ProviderUsageObserver: Send + Sync {
    fn observe_usage(&self, usage: ProviderUsage) -> impl Future<Output = io::Result<()>> + Send;
}
