use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;

use codex_infra_account::AccountConnectionConfig;
use codex_infra_account::AccountReplicaConfig;
use codex_infra_extension::AgentAccountSource;
use codex_infra_protocol::MachineId;
use codex_infra_runtime::LaunchIntent;
use codex_infra_runtime::MachineLaunchConfig;
use codex_infra_state::ArchiveStream;
use serde::Deserialize;

use super::run::RunConfig;

#[derive(Deserialize)]
pub struct AccountDiscovery {
    owner_machine: MachineId,
    directories_file: PathBuf,
    page_size: NonZeroUsize,
    batch_size: NonZeroUsize,
    poll_interval_ms: NonZeroU64,
    connection: AccountConnectionConfig,
}

pub fn resolve(
    config: &RunConfig,
    launch: &LaunchIntent,
    directory: &Path,
) -> io::Result<AgentAccountSource> {
    let mut source = if let Some(discovery) = &config.account_discovery {
        let bytes = std::fs::read(&discovery.directories_file)?;
        let streams: BTreeMap<MachineId, ArchiveStream> = serde_json::from_slice(&bytes)?;
        let stream = streams
            .get(&discovery.owner_machine)
            .ok_or_else(|| {
                io::Error::other(
                    "account owner directory is not yet published; refresh machine discovery",
                )
            })?
            .clone();
        let machine = MachineLaunchConfig::read(
            &launch
                .generation
                .config_store_path
                .join("machine-runtime.json"),
        )?;
        std::fs::write(directory.join("account-discovery.json"), bytes)?;
        AgentAccountSource::Published {
            replica: Box::new(AccountReplicaConfig {
                git: machine.programs.git,
                repository: machine.team_state_repository,
                remote: machine.archive_remote,
                directory: directory.join("account-replica"),
                stream,
                page_size: discovery.page_size,
                batch_size: discovery.batch_size,
                poll_interval_ms: discovery.poll_interval_ms,
            }),
            connection: discovery.connection.clone(),
        }
    } else {
        config
            .account
            .clone()
            .unwrap_or(AgentAccountSource::Generation)
    };
    if let AgentAccountSource::Published { replica, .. } = &mut source {
        replica.directory = directory.join("account-replica");
    }
    std::fs::write(
        directory.join("account-source.json"),
        serde_json::to_vec_pretty(&source)?,
    )?;
    Ok(source)
}
