use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use codex_infra_account::AccountDirectory;
use codex_infra_account::AccountDirectoryUpdate;

use crate::ArchiveActor;
use crate::ArchiveController;
use crate::CollectorArchiveActor;
use crate::MachineArchiveWriter;
use crate::ProviderArchiveActor;
use crate::ProviderArchiveConfig;
use crate::SessionActor;
use crate::SessionController;
use crate::ShardArchiveActor;
use crate::TransportArchiveActor;
use crate::TransportSession;

mod account_archive;
mod accounts;
mod exchange_archive;
use account_archive::AccountArchiveActor;
use exchange_archive::ExchangeArchiveActor;
use exchange_archive::ExchangeArchiveConfig;
mod generation;
mod launch;
pub use accounts::MachineAccountConfig;
use accounts::MachineAccountServices;
pub use accounts::MachineAccountServicesConfig;
pub use generation::MachineLaunchProvenance;
pub use launch::MachineLaunchConfig;
pub use launch::MachinePrograms;
pub use launch::MachineScheduling;

pub struct MachineRuntimeConfig {
    pub accounts: MachineAccountServicesConfig,
    pub provider_archives: ProviderArchiveConfig,
    pub transport_interval: Duration,
    pub archive_interval: Duration,
    pub command_capacity: NonZeroUsize,
}

/// Owns one machine's transport and archive services for a Root Session. Agent
/// processes retain independent tmux lifetimes; this is not a global scheduler.
/// Startup keeps partial progress in this owner, allowing retries or shutdown.
pub struct MachineRuntime {
    config: MachineRuntimeConfig,
    directory: PathBuf,
    endpoint: SocketAddr,
    session: Option<TransportSession>,
    transport: Option<SessionActor>,
    writer: Option<MachineArchiveWriter>,
    archive: Option<ArchiveActor>,
    collectors: Option<CollectorArchiveActor>,
    snapshots: Option<TransportArchiveActor>,
    shards: Option<ShardArchiveActor>,
    providers: Option<ProviderArchiveActor>,
    accounts: MachineAccountServices,
    account_directory: Option<AccountDirectory>,
    account_updates: Vec<AccountDirectoryUpdate>,
    account_archive: Option<AccountArchiveActor>,
    exchange_archive: Option<ExchangeArchiveActor>,
}

/// Resources returned for final snapshots, backlog reconciliation and restart.
/// A successful service shutdown alone does not establish Agent/Session completion.
pub struct MachineRuntimeExit {
    pub account_updates: Vec<AccountDirectoryUpdate>,
    pub session: Option<TransportSession>,
    pub writer: Option<MachineArchiveWriter>,
    pub failures: Vec<String>,
}

impl MachineRuntime {
    pub fn new(
        session: TransportSession,
        writer: MachineArchiveWriter,
        config: MachineRuntimeConfig,
    ) -> Self {
        Self {
            directory: session.spool_directory().to_path_buf(),
            endpoint: session.endpoint(),
            config,
            session: Some(session),
            transport: None,
            writer: Some(writer),
            archive: None,
            collectors: None,
            snapshots: None,
            shards: None,
            providers: None,
            accounts: MachineAccountServices::default(),
            account_directory: None,
            account_updates: Vec::new(),
            account_archive: None,
            exchange_archive: None,
        }
    }

    /// Cancellation while opening a service leaves all previously started
    /// services owned here. Retry resumes at the first service still absent.
    pub async fn start(&mut self) -> io::Result<()> {
        if self.config.transport_interval.is_zero() || self.config.archive_interval.is_zero() {
            return Err(io::Error::other(
                "machine service intervals must be positive",
            ));
        }
        if self.archive.is_none() {
            let writer = self
                .writer
                .take()
                .ok_or_else(|| io::Error::other("machine archive writer missing"))?;
            self.archive = Some(ArchiveActor::start(
                writer,
                self.config.archive_interval,
                self.config.command_capacity,
            )?);
        }
        let archive = self.archive_controller()?;
        if self.collectors.is_none() {
            let session = self.session.as_ref().ok_or_else(|| {
                io::Error::other("transport moved before collector archive startup")
            })?;
            self.collectors = Some(
                session
                    .start_collector_archiver(archive.clone(), self.config.archive_interval)
                    .await?,
            );
        }
        if self.transport.is_none() {
            let session = self
                .session
                .take()
                .ok_or_else(|| io::Error::other("machine transport missing"))?;
            self.transport = Some(SessionActor::start(
                session,
                self.config.transport_interval,
                self.config.command_capacity,
            )?);
        }
        let session = self.controller()?;
        if self.snapshots.is_none() {
            self.snapshots = Some(
                TransportArchiveActor::start(
                    session.clone(),
                    archive.clone(),
                    self.directory.join("transport-archive"),
                    self.config.archive_interval,
                )
                .await?,
            );
        }
        if self.shards.is_none() {
            self.shards = Some(
                ShardArchiveActor::start(
                    session,
                    archive.clone(),
                    self.directory.clone(),
                    self.directory.join("shard-archive"),
                    self.config.archive_interval,
                )
                .await?,
            );
        }
        if self.providers.is_none() {
            self.providers = Some(
                ProviderArchiveActor::start(self.config.provider_archives.clone(), archive).await?,
            );
        }
        if self.account_directory.is_none() {
            let path = self.directory.join("account-directory.journal");
            self.account_directory = Some(
                tokio::task::spawn_blocking(move || AccountDirectory::open(&path))
                    .await
                    .map_err(io::Error::other)??,
            );
        }
        self.accounts.start(&self.config.accounts).await?;
        if self.exchange_archive.is_none() {
            self.exchange_archive = Some(
                ExchangeArchiveActor::start(
                    ExchangeArchiveConfig {
                        root_session_id: self.config.accounts.root_session_id,
                        machine_id: self.config.accounts.machine_id.clone(),
                        exchanges: self.config.accounts.audit_directory.clone(),
                        directory: self.directory.join("account-exchange-archive"),
                        interval: self.config.archive_interval,
                    },
                    self.archive_controller()?,
                )
                .await?,
            );
        }
        self.account_updates = self.accounts.publish(&self.account_directory()?).await?;
        if self.account_archive.is_none() {
            self.account_archive = Some(
                AccountArchiveActor::start(
                    self.account_directory()?,
                    self.archive_controller()?,
                    self.config.provider_archives.root_session_id,
                    self.config.accounts.machine_id.clone(),
                    self.directory.join("account-archive"),
                    self.config.archive_interval,
                )
                .await?,
            );
        }
        Ok(())
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub fn account_updates(&self) -> Vec<AccountDirectoryUpdate> {
        self.account_updates.clone()
    }

    pub fn account_directory(&self) -> io::Result<AccountDirectory> {
        self.account_directory
            .clone()
            .ok_or_else(|| io::Error::other("machine account directory has not opened"))
    }

    pub fn account_archive_state(&self) -> io::Result<crate::ArchiveWorkerState> {
        self.account_archive
            .as_ref()
            .map(AccountArchiveActor::state)
            .ok_or_else(|| io::Error::other("account archive actor has not started"))
    }

    pub fn account_archive_stream(&self) -> io::Result<codex_infra_state::ArchiveStream> {
        self.account_archive
            .as_ref()
            .map(AccountArchiveActor::stream)
            .ok_or_else(|| io::Error::other("account archive actor has not started"))
    }

    pub fn account_exchange_archive_state(&self) -> io::Result<crate::ArchiveWorkerState> {
        self.exchange_archive
            .as_ref()
            .map(ExchangeArchiveActor::state)
            .ok_or_else(|| io::Error::other("account exchange archive actor has not started"))
    }

    pub fn controller(&self) -> io::Result<SessionController> {
        self.transport
            .as_ref()
            .map(SessionActor::controller)
            .ok_or_else(|| io::Error::other("machine transport actor has not started"))
    }

    pub fn archive_controller(&self) -> io::Result<ArchiveController> {
        self.archive
            .as_ref()
            .map(ArchiveActor::controller)
            .ok_or_else(|| io::Error::other("machine archive actor has not started"))
    }

    /// The caller arranges final Agent output before shutting down its observer.
    /// An owned task continues service teardown if this waiter is canceled.
    /// Failures are returned with any resources still available for recovery.
    pub async fn stop(mut self) -> io::Result<MachineRuntimeExit> {
        tokio::spawn(async move {
            let mut failures = Vec::new();
            let account_updates = if let Some(directory) = &self.account_directory {
                let (updates, withdrawal_failures) = self.accounts.withdraw(directory).await;
                failures.extend(withdrawal_failures);
                updates
            } else {
                Vec::new()
            };
            failures.extend(std::mem::take(&mut self.accounts).stop().await);
            if let Some(exchange_archive) = self.exchange_archive.take()
                && let Err(error) = exchange_archive.stop().await
            {
                failures.push(format!("account exchange archive: {error}"));
            }
            if let Some(account_archive) = self.account_archive.take()
                && let Err(error) = account_archive.stop().await
            {
                failures.push(format!("account directory archive: {error}"));
            }
            if let Some(transport) = self.transport.take() {
                match transport.stop().await {
                    Ok(exit) => {
                        self.session = Some(exit.session);
                        failures.extend(exit.failures);
                    }
                    Err(error) => failures.push(format!("transport: {error}")),
                }
            }
            if let Some(session) = &mut self.session
                && let Err(error) = session.stop_capture(self.config.transport_interval).await
            {
                failures.push(format!("capture: {error}"));
            }
            if let Some(snapshots) = self.snapshots.take()
                && let Err(error) = snapshots.stop().await
            {
                failures.push(format!("transport archives: {error}"));
            }
            if let Some(shards) = self.shards.take()
                && let Err(error) = shards.stop().await
            {
                failures.push(format!("shard archives: {error}"));
            }
            if let Some(collectors) = self.collectors.take()
                && let Err(error) = collectors.stop().await
            {
                failures.push(format!("collector archives: {error}"));
            }
            if let Some(providers) = self.providers.take()
                && let Err(error) = providers.stop().await
            {
                failures.push(format!("provider archives: {error}"));
            }
            if let Some(archive) = self.archive.take() {
                match archive.stop().await {
                    Ok(writer) => self.writer = Some(writer),
                    Err(error) => failures.push(format!("archive writer: {error}")),
                }
            }
            MachineRuntimeExit {
                account_updates,
                session: self.session,
                writer: self.writer,
                failures,
            }
        })
        .await
        .map_err(io::Error::other)
    }
}
