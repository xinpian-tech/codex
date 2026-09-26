use std::fs;
use std::io;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::ConfigGeneration;

use crate::LockedFlake;
use crate::NixTaskResolver;
use crate::ResolvedTask;

impl NixTaskResolver {
    /// Realizes the planned Team State generation. Its `generation.json` contains
    /// fixed input revisions/configuration metadata, excluding its own drv/store
    /// paths. These two fields are attached from actual Nix build output here.
    pub fn realize_generation(
        &self,
        task: &ResolvedTask,
        source: &LockedFlake,
    ) -> io::Result<ConfigGeneration> {
        if self.machine_id()? != task.task.target_machine {
            return Err(io::Error::other("generation Task targets another hostid"));
        }
        let team_commit: CommitId = task.flake_revision.parse().map_err(io::Error::other)?;
        let source_commit: CommitId = source.revision.parse().map_err(io::Error::other)?;
        let lock: serde_json::Value =
            serde_json::from_slice(&fs::read(task.source_store_path.join("flake.lock"))?)
                .map_err(io::Error::other)?;
        let source_is_locked = lock
            .get("nodes")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|nodes| {
                nodes.values().any(|node| {
                    node.get("locked")
                        .and_then(|locked| locked.get("rev"))
                        .and_then(serde_json::Value::as_str)
                        == Some(source.revision.as_str())
                })
            });
        if !source_is_locked {
            return Err(io::Error::other(
                "Team State flake lock does not reference the selected Source commit",
            ));
        }
        let plan = task.build_plan.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "generation Task must declare its build derivation",
            )
        })?;
        let outputs = self.build(plan)?;
        let [output] = outputs.as_slice() else {
            return Err(io::Error::other(
                "generation must realize exactly one derivation",
            ));
        };
        if output.drv_path != plan.derivation {
            return Err(io::Error::other(
                "generation output differs from planned derivation",
            ));
        }
        let store_path = output
            .outputs
            .get("out")
            .ok_or_else(|| io::Error::other("generation out output missing"))?;
        let mut manifest: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&fs::read(store_path.join("generation.json"))?)
                .map_err(io::Error::other)?;
        if manifest.contains_key("config_derivation") || manifest.contains_key("config_store_path")
        {
            return Err(io::Error::other(
                "generation manifest must omit its own drv and output paths",
            ));
        }
        manifest.insert(
            "config_derivation".to_owned(),
            serde_json::to_value(&output.drv_path).map_err(io::Error::other)?,
        );
        manifest.insert(
            "config_store_path".to_owned(),
            serde_json::to_value(store_path).map_err(io::Error::other)?,
        );
        let generation: ConfigGeneration =
            serde_json::from_value(manifest.into()).map_err(io::Error::other)?;
        if generation.team_state_commit != team_commit
            || generation.codex_source_commit != source_commit
            || generation.source_flake_lock_hash != source.lock_hash
            || generation.state_flake_lock_hash != task.flake_lock_hash
            || generation.nix_system != task.task.nix_system
        {
            return Err(io::Error::other(
                "generation manifest differs from locked source/state inputs",
            ));
        }
        // The flake emits the effective config alongside the manifest. Hash the
        // actual file rather than accepting a caller-supplied configuration digest.
        let config = fs::read(store_path.join("config.toml"))?;
        if generation.effective_config_digest != format!("blake3:{}", blake3::hash(&config)) {
            return Err(io::Error::other(
                "generation effective config digest differs from its output",
            ));
        }
        Ok(generation)
    }
}
