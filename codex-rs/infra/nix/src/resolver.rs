use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::Command;

use codex_infra_protocol::BuildIntent;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::TaskSpec;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildPlan {
    pub attribute: String,
    pub derivation: PathBuf,
    pub outputs: BTreeMap<String, Option<PathBuf>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedTask {
    pub task: TaskSpec,
    pub locked_flake_reference: String,
    pub source_store_path: PathBuf,
    pub flake_revision: String,
    pub flake_lock_hash: String,
    pub build_plan: Option<BuildPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockedFlake {
    pub reference: String,
    pub store_path: PathBuf,
    pub revision: String,
    pub lock_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildOutput {
    pub drv_path: PathBuf,
    pub outputs: BTreeMap<String, PathBuf>,
}

#[derive(Deserialize)]
struct FlakeMetadata {
    url: String,
    path: PathBuf,
    locked: LockedInput,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LockedInput {
    rev: Option<String>,
    nar_hash: String,
}

#[derive(Deserialize)]
struct Derivation {
    system: String,
    outputs: BTreeMap<String, DerivationOutput>,
}

#[derive(Deserialize)]
struct DerivationOutput {
    path: Option<PathBuf>,
}

/// Programs are supplied from the machine's deployed Nix generation.
pub struct NixTaskResolver {
    nix: PathBuf,
    hostid: PathBuf,
}

impl NixTaskResolver {
    pub fn new(nix: PathBuf, hostid: PathBuf) -> Self {
        Self { nix, hostid }
    }

    pub fn machine_id(&self) -> io::Result<MachineId> {
        let output = Command::new(&self.hostid).output()?;
        if !output.status.success() {
            return Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        std::str::from_utf8(&output.stdout)
            .map_err(io::Error::other)?
            .trim()
            .parse()
            .map_err(io::Error::other)
    }

    /// Evaluation chooses immutable inputs and a drv; it does not realize builds.
    pub fn resolve(&self, task: TaskSpec) -> io::Result<ResolvedTask> {
        if self.machine_id()? != task.target_machine {
            return Err(io::Error::other(
                "Task target differs from the resolver's hostid",
            ));
        }
        let metadata = self.lock_flake(&task.flake_reference)?;
        let build_plan = match &task.build {
            BuildIntent::NoBuild => None,
            BuildIntent::Derivation { attribute } => {
                let installable = format!("{}#{attribute}.drvPath", metadata.reference);
                let derivation: PathBuf = self.json(&[
                    "eval",
                    "--json",
                    "--no-write-lock-file",
                    "--no-update-lock-file",
                    "--system",
                    &task.nix_system,
                    "--",
                    &installable,
                ])?;
                let definition: BTreeMap<PathBuf, Derivation> =
                    self.json(&["derivation", "show", "--", &derivation.to_string_lossy()])?;
                let definition = definition.get(&derivation).ok_or_else(|| {
                    io::Error::other("resolved derivation missing from Nix output")
                })?;
                if definition.system != task.nix_system {
                    return Err(io::Error::other(
                        "build derivation system differs from Task Nix system",
                    ));
                }
                Some(BuildPlan {
                    attribute: attribute.clone(),
                    derivation,
                    outputs: definition
                        .outputs
                        .iter()
                        .map(|(name, output)| (name.clone(), output.path.clone()))
                        .collect(),
                })
            }
        };
        Ok(ResolvedTask {
            task,
            locked_flake_reference: metadata.reference,
            source_store_path: metadata.store_path,
            flake_revision: metadata.revision,
            flake_lock_hash: metadata.lock_hash,
            build_plan,
        })
    }

    pub fn lock_flake(&self, reference: &str) -> io::Result<LockedFlake> {
        let metadata: FlakeMetadata = self.json(&[
            "flake",
            "metadata",
            "--json",
            "--no-write-lock-file",
            "--no-update-lock-file",
            "--",
            reference,
        ])?;
        let lock = fs::read(metadata.path.join("flake.lock"))?;
        Ok(LockedFlake {
            reference: metadata.url,
            store_path: metadata.path,
            revision: metadata.locked.rev.unwrap_or(metadata.locked.nar_hash),
            lock_hash: format!("blake3:{}", blake3::hash(&lock)),
        })
    }

    /// Realizes the exact planned drv, rather than re-resolving a mutable flake ref.
    pub fn build(&self, plan: &BuildPlan) -> io::Result<Vec<BuildOutput>> {
        let installable = format!("{}^*", plan.derivation.display());
        self.json(&["build", "--json", "--no-link", "--", &installable])
    }

    fn json<T: serde::de::DeserializeOwned>(&self, args: &[&str]) -> io::Result<T> {
        let output = Command::new(&self.nix).args(args).output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "Nix exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        serde_json::from_slice(&output.stdout).map_err(io::Error::other)
    }
}
