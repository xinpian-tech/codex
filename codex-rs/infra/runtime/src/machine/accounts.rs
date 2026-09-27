use std::collections::BTreeSet;
use std::io;
use std::net::IpAddr;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use codex_infra_account::AccountAuthority;
use codex_infra_account::AccountOwnerAssignment;
use codex_infra_account::AccountService;
use codex_infra_account::AccountServiceConfig;
use codex_infra_account::CodexRefreshConfig;
use codex_infra_account::GitAccountOwner;
use codex_infra_account::GitAccounts;
use codex_infra_protocol::CommitId;
use codex_infra_protocol::MachineId;
use codex_login::AuthRouteConfig;
use serde::Deserialize;
use serde::Serialize;

/// Team State account assignment served on this machine. The directory is
/// shared across Root Sessions, so refresh journals have one machine-local home.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineAccountConfig {
    pub provider_id: String,
    pub account_id: String,
    pub owner_revision: CommitId,
    pub directory: PathBuf,
    pub remote: String,
    pub config_ref: String,
    pub chatgpt_base_url: String,
    pub concurrent_requests: NonZeroUsize,
    pub frame_bytes: NonZeroUsize,
    pub io_timeout_ms: NonZeroU64,
}

/// Actual dynamically allocated account endpoint, suitable for the directory
/// publisher and RemoteAccountSource's authority feed.
#[derive(Clone, Serialize, Deserialize)]
pub struct MachineAccountEndpoint {
    pub provider_id: String,
    pub account_id: String,
    pub authority: AccountAuthority,
}

pub struct MachineAccountServicesConfig {
    pub git: PathBuf,
    pub repository: PathBuf,
    pub machine_id: MachineId,
    pub bind_address: IpAddr,
    pub accounts: Vec<MachineAccountConfig>,
}

#[derive(Default)]
pub(super) struct MachineAccountServices {
    active: Vec<(MachineAccountEndpoint, AccountService)>,
}

impl MachineAccountServices {
    pub(super) async fn start(&mut self, config: &MachineAccountServicesConfig) -> io::Result<()> {
        let mut identities = BTreeSet::new();
        for account in &config.accounts {
            if !identities.insert((&account.provider_id, &account.account_id)) {
                return Err(io::Error::other("duplicate machine account assignment"));
            }
            if !account.directory.is_absolute() {
                return Err(io::Error::other(
                    "machine account directory must be absolute",
                ));
            }
        }
        // Completed starts stay owned here across cancellation or a later error.
        for account in &config.accounts[self.active.len()..] {
            let source = GitAccountOwner {
                git: GitAccounts {
                    git: config.git.clone(),
                    repository: config.repository.clone(),
                    remote: account.remote.clone(),
                    config_ref: account.config_ref.clone(),
                },
                assignment: AccountOwnerAssignment {
                    provider_id: account.provider_id.clone(),
                    account_id: account.account_id.clone(),
                    machine_id: config.machine_id.clone(),
                },
                owner_revision: account.owner_revision.clone(),
                directory: account.directory.join("publication"),
                refresh: CodexRefreshConfig {
                    directory: account.directory.join("oauth"),
                    chatgpt_base_url: account.chatgpt_base_url.clone(),
                    auth_route: AuthRouteConfig::from_http_client_factory(HttpClientFactory::new(
                        OutboundProxyPolicy::ReqwestDefault,
                    )),
                },
            };
            let service = AccountService::start(
                AccountServiceConfig {
                    machine_id: config.machine_id.clone(),
                    owner_revision: account.owner_revision.clone(),
                    bind_ip: config.bind_address,
                    provider_id: account.provider_id.clone(),
                    account_id: account.account_id.clone(),
                    concurrent_requests: account.concurrent_requests,
                    frame_bytes: account.frame_bytes,
                    io_timeout: Duration::from_millis(account.io_timeout_ms.get()),
                },
                source,
            )
            .await?;
            self.active.push((
                MachineAccountEndpoint {
                    provider_id: account.provider_id.clone(),
                    account_id: account.account_id.clone(),
                    authority: service.authority().clone(),
                },
                service,
            ));
        }
        Ok(())
    }

    pub(super) fn endpoints(&self) -> Vec<MachineAccountEndpoint> {
        self.active
            .iter()
            .map(|(endpoint, _)| endpoint.clone())
            .collect()
    }

    pub(super) async fn stop(self) -> Vec<String> {
        let mut stopping = tokio::task::JoinSet::new();
        for (endpoint, service) in self.active {
            stopping.spawn(async move { (endpoint, service.stop().await) });
        }
        let mut failures = Vec::new();
        while let Some(result) = stopping.join_next().await {
            match result {
                Ok((endpoint, Err(error))) => failures.push(format!(
                    "account {}/{}: {error}",
                    endpoint.provider_id, endpoint.account_id
                )),
                Ok((_, Ok(()))) => {}
                Err(error) => failures.push(format!("account shutdown: {error}")),
            }
        }
        failures
    }
}
