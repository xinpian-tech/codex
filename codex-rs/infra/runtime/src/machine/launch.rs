use std::fs;
use std::io;
use std::net::IpAddr;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use codex_infra_protocol::MachineId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::SessionShard;
use codex_infra_tmux::TmuxClient;
use serde::Deserialize;
use serde::Serialize;

use super::MachineAccountConfig;
use super::MachineAccountServicesConfig;
use super::MachineRuntime;
use super::MachineRuntimeConfig;
use crate::MachineArchiveWriter;
use crate::ProviderArchiveConfig;
use crate::TransportSession;
use crate::TransportSessionConfig;

/// Executable and tmux configuration paths supplied by the realized Nix
/// generation. The hostid executable reports the machine actually opening it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachinePrograms {
    pub hostid: PathBuf,
    pub git: PathBuf,
    pub tmux: PathBuf,
    pub tmux_config: PathBuf,
    pub keeper: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineScheduling {
    pub event_batch: NonZeroUsize,
    pub receive_capacity: NonZeroUsize,
    pub command_capacity: NonZeroUsize,
    pub send_concurrency: NonZeroUsize,
    pub input_concurrency: NonZeroUsize,
    pub archive_chunk_bytes: NonZeroUsize,
    pub send_timeout_ms: NonZeroU64,
    pub retry_delay_ms: NonZeroU64,
    pub transport_interval_ms: NonZeroU64,
    pub archive_interval_ms: NonZeroU64,
}

/// Machine launch data stored in the separate Team State flake. Source paths
/// refer to the realized generation; mutable spool and Team State checkout paths
/// are host-local. The gateway port is allocated dynamically by TransportSession.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineLaunchConfig {
    pub root_session_id: RootSessionId,
    pub machine_id: MachineId,
    pub spool_directory: PathBuf,
    pub team_state_repository: PathBuf,
    pub archive_remote: String,
    pub tmux_socket: PathBuf,
    pub bind_address: IpAddr,
    pub programs: MachinePrograms,
    pub scheduling: MachineScheduling,
    #[serde(default)]
    pub accounts: Vec<MachineAccountConfig>,
}

impl MachineLaunchConfig {
    pub fn read(path: &Path) -> io::Result<Self> {
        serde_json::from_slice(&fs::read(path)?).map_err(io::Error::other)
    }

    /// Opens durable resources and capture, but leaves service startup explicit
    /// on the returned MachineRuntime so the CLI owns partial startup outcomes.
    pub async fn open(self) -> io::Result<MachineRuntime> {
        let prepared = tokio::task::spawn_blocking(move || {
            let output = Command::new(&self.programs.hostid).output()?;
            if !output.status.success() {
                return Err(io::Error::other(format!("hostid exited {}", output.status)));
            }
            let machine_id: MachineId = std::str::from_utf8(&output.stdout)
                .map_err(io::Error::other)?
                .trim()
                .parse()
                .map_err(io::Error::other)?;
            if machine_id != self.machine_id {
                return Err(io::Error::other(
                    "machine launch configuration differs from current hostid",
                ));
            }
            fs::create_dir_all(&self.spool_directory)?;
            let spool = self.spool_directory.canonicalize()?;
            let archive = spool.join("archive");
            fs::create_dir_all(&archive)?;
            let shard = SessionShard::open(
                self.programs.git.clone(),
                self.team_state_repository.canonicalize()?,
                archive.join("git-index"),
                self.archive_remote.clone(),
                self.root_session_id,
                &machine_id,
            )?;
            let writer = MachineArchiveWriter::open(
                shard,
                &archive.join("jobs.journal"),
                self.scheduling.archive_chunk_bytes,
            )?;
            Ok::<_, io::Error>((self, spool, writer))
        })
        .await
        .map_err(io::Error::other)??;
        let (config, spool, writer) = prepared;
        let scheduling = config.scheduling;
        let session = TransportSession::open(
            TransportSessionConfig {
                directory: spool.join("transport"),
                root_session_id: config.root_session_id,
                machine_id: config.machine_id.clone(),
                bind_address: config.bind_address,
                event_batch: scheduling.event_batch,
                receive_capacity: scheduling.receive_capacity,
                send_concurrency: scheduling.send_concurrency,
                input_concurrency: scheduling.input_concurrency,
                send_timeout: Duration::from_millis(scheduling.send_timeout_ms.get()),
                retry_delay: Duration::from_millis(scheduling.retry_delay_ms.get()),
            },
            Arc::new(TmuxClient::new(
                config.programs.tmux,
                config.tmux_socket,
                config.programs.tmux_config,
                config.programs.keeper,
            )),
        )
        .await?;
        Ok(MachineRuntime::new(
            session,
            writer,
            MachineRuntimeConfig {
                accounts: MachineAccountServicesConfig {
                    git: config.programs.git,
                    repository: config.team_state_repository,
                    machine_id: config.machine_id.clone(),
                    bind_address: config.bind_address,
                    accounts: config.accounts,
                },
                provider_archives: ProviderArchiveConfig {
                    root_session_id: config.root_session_id,
                    machine_id: config.machine_id,
                    attempts: spool.join("provider-attempts"),
                    directory: spool.join("provider-archive"),
                    interval: Duration::from_millis(scheduling.archive_interval_ms.get()),
                },
                transport_interval: Duration::from_millis(scheduling.transport_interval_ms.get()),
                archive_interval: Duration::from_millis(scheduling.archive_interval_ms.get()),
                command_capacity: scheduling.command_capacity,
            },
        ))
    }
}
