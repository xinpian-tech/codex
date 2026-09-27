use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_protocol::ConfigGeneration;

use super::AgentHostGeneration;

/// Called under the launch's preparation journal ownership. Publish the complete
/// initial home at once; subsequent openings preserve auth, sessions and caches.
pub(super) fn prepare_home(
    directory: &Path,
    generation: &ConfigGeneration,
    inputs: &AgentHostGeneration,
) -> io::Result<PathBuf> {
    let home = directory.join("home");
    let marker = "infra-generation.json";
    if home.exists() {
        let previous: ConfigGeneration = serde_json::from_slice(&fs::read(home.join(marker))?)?;
        if previous != *generation {
            return Err(io::Error::other("Agent home belongs to another generation"));
        }
        return Ok(home);
    }
    // Only this unpublished staging directory is rebuilt after interrupted copy.
    let staging = directory.join("home.pending");
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir(&staging)?;
    fs::write(staging.join("config.toml"), &inputs.effective_config_bytes)?;
    fs::File::open(staging.join("config.toml"))?.sync_all()?;
    for (source, destination) in [
        ("skills", "skills"),
        ("memory", "memories"),
        ("roles", "roles"),
    ] {
        copy_tree(
            &generation.config_store_path.join(source),
            &staging.join(destination),
            &mut Vec::new(),
        )?;
    }
    fs::write(staging.join(marker), serde_json::to_vec(generation)?)?;
    fs::File::open(staging.join(marker))?.sync_all()?;
    #[cfg(unix)]
    fs::File::open(&staging)?.sync_all()?;
    fs::rename(&staging, &home)?;
    #[cfg(unix)]
    fs::File::open(directory)?.sync_all()?;
    Ok(home)
}

// Resolve generation symlinks while copying so every Agent gets writable local
// files, including skill scripts. Track ancestors to report directory cycles.
fn copy_tree(source: &Path, destination: &Path, ancestors: &mut Vec<PathBuf>) -> io::Result<()> {
    let source = source.canonicalize()?;
    let metadata = fs::metadata(&source)?;
    if metadata.is_dir() {
        if ancestors.contains(&source) {
            return Err(io::Error::other("cycle in generation knowledge directory"));
        }
        fs::create_dir(destination)?;
        ancestors.push(source.clone());
        for entry in fs::read_dir(&source)? {
            let entry = entry?;
            copy_tree(
                &entry.path(),
                &destination.join(entry.file_name()),
                ancestors,
            )?;
        }
        ancestors.pop();
        #[cfg(unix)]
        fs::File::open(destination)?.sync_all()?;
    } else if metadata.is_file() {
        fs::copy(&source, destination)?;
        let mut permissions = metadata.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            permissions.set_mode(permissions.mode() | 0o200);
        }
        #[cfg(not(unix))]
        permissions.set_readonly(false);
        fs::set_permissions(destination, permissions)?;
        fs::File::open(destination)?.sync_all()?;
    } else {
        return Err(io::Error::other(
            "generation knowledge entry is not a file or directory",
        ));
    }
    Ok(())
}
