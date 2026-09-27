use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use codex_infra_protocol::ConfigGeneration;
use serde::Deserialize;
use serde::Serialize;

use super::AgentPreparationConfig;

/// Host-local settings supplied by agent-host.json in the Team State generation.
/// Agent, Task and provider identities remain in the machine's LaunchIntent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHostConfig {
    pub preparation: AgentPreparationConfig,
    pub programs: AgentHostPrograms,
    pub input_batch: NonZeroUsize,
    pub channel_capacity: NonZeroUsize,
    pub provider_audit_directory: PathBuf,
    pub account_exchange_directory: PathBuf,
}

/// Codex helper entrypoints from the deployed source flake, separate from the
/// independent Agent host executable selected by LaunchIntent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHostPrograms {
    pub codex: PathBuf,
    pub linux_sandbox: Option<PathBuf>,
    pub execve_wrapper: Option<PathBuf>,
}

/// Exact generation inputs retained with the prepared launch for later archive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHostGeneration {
    pub config: AgentHostConfig,
    pub config_path: PathBuf,
    pub config_bytes: Vec<u8>,
    pub manifest_bytes: Vec<u8>,
    pub effective_config_bytes: Vec<u8>,
}

impl AgentHostGeneration {
    /// Reads only the generation selected by the launch, comparing all recorded
    /// input revisions and inference binding with the realized manifest.
    pub fn read(generation: &ConfigGeneration) -> io::Result<Self> {
        let directory = &generation.config_store_path;
        let config_path = directory.join("agent-host.json");
        let config_bytes = fs::read(&config_path)?;
        let manifest_bytes = fs::read(directory.join("generation.json"))?;
        let mut manifest: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&manifest_bytes)?;
        let digest = format!("blake3:{}", blake3::hash(&config_bytes));
        if manifest
            .get("agent_host_digest")
            .and_then(serde_json::Value::as_str)
            != Some(digest.as_str())
        {
            return Err(io::Error::other("Agent host configuration digest changed"));
        }
        if manifest.contains_key("config_derivation") || manifest.contains_key("config_store_path")
        {
            return Err(io::Error::other(
                "generation manifest contains its output paths",
            ));
        }
        manifest.insert(
            "config_derivation".to_owned(),
            serde_json::to_value(&generation.config_derivation)?,
        );
        manifest.insert(
            "config_store_path".to_owned(),
            serde_json::to_value(directory)?,
        );
        let actual: ConfigGeneration = serde_json::from_value(manifest.into())?;
        if actual != *generation {
            return Err(io::Error::other(
                "Agent generation differs from launch binding",
            ));
        }
        let effective_config_bytes = fs::read(directory.join("config.toml"))?;
        if generation.effective_config_digest
            != format!("blake3:{}", blake3::hash(&effective_config_bytes))
        {
            return Err(io::Error::other(
                "effective Codex configuration digest changed",
            ));
        }
        Ok(Self {
            config: serde_json::from_slice(&config_bytes)?,
            config_path,
            config_bytes,
            manifest_bytes,
            effective_config_bytes,
        })
    }
}
