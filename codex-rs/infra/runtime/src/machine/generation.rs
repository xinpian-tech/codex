use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_protocol::CommitId;
use serde::Deserialize;
use serde::Serialize;

use super::MachineLaunchConfig;

/// Records the generation files actually consumed by the machine CLI. Derivation
/// realization remains owned by the Nix resolver; no derivation path is inferred
/// from a generation directory name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineLaunchProvenance {
    pub config_path: PathBuf,
    pub config_bytes: Vec<u8>,
    pub generation_store_path: PathBuf,
    pub manifest_bytes: Vec<u8>,
    pub effective_config_bytes: Vec<u8>,
    pub codex_source_commit: CommitId,
    pub team_state_commit: CommitId,
    pub nix_system: String,
}

#[derive(Deserialize)]
struct Manifest {
    codex_source_commit: CommitId,
    team_state_commit: CommitId,
    nix_system: String,
    machine_runtime_digest: String,
    effective_config_digest: String,
}

impl MachineLaunchConfig {
    /// Reads one realized generation, retaining the exact manifest/config bytes.
    /// Machine and effective Codex configuration digests are emitted by the same
    /// Team State derivation, so startup can identify the actual consumed pair.
    pub fn read_generation(path: &Path) -> io::Result<(Self, MachineLaunchProvenance)> {
        let config_path = path.canonicalize()?;
        let generation_store_path = config_path
            .parent()
            .ok_or_else(|| io::Error::other("machine generation directory missing"))?
            .to_path_buf();
        let config_bytes = fs::read(&config_path)?;
        let manifest_bytes = fs::read(generation_store_path.join("generation.json"))?;
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
        if manifest.machine_runtime_digest != format!("blake3:{}", blake3::hash(&config_bytes)) {
            return Err(io::Error::other(
                "machine configuration differs from generation digest",
            ));
        }
        let effective_config_bytes = fs::read(generation_store_path.join("config.toml"))?;
        if manifest.effective_config_digest
            != format!("blake3:{}", blake3::hash(&effective_config_bytes))
        {
            return Err(io::Error::other(
                "effective configuration differs from generation digest",
            ));
        }
        let config = serde_json::from_slice(&config_bytes)?;
        Ok((
            config,
            MachineLaunchProvenance {
                config_path,
                config_bytes,
                generation_store_path,
                manifest_bytes,
                effective_config_bytes,
                codex_source_commit: manifest.codex_source_commit,
                team_state_commit: manifest.team_state_commit,
                nix_system: manifest.nix_system,
            },
        ))
    }
}
