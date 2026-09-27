use std::io;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::time::Duration;

use codex_infra_protocol::CommitId;
use codex_login::ExternalAuthRefreshContext;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::watch;

use crate::AccountCredentialSource;
use crate::AccountDirectoryReplica;
use crate::AccountReplicaConfig;
use crate::AccountReplicaState;
use crate::PublishedAccount;
use crate::RemoteAccountSource;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountConnectionConfig {
    pub frame_bytes: NonZeroUsize,
    pub request_timeout_ms: NonZeroU64,
    pub initial_sync_timeout_ms: NonZeroU64,
}

/// Published credentials reached through an Agent-owned directory replica.
/// Retaining this source keeps discovery running for every AuthManager using it.
pub struct ReplicatedAccountSource {
    remote: RemoteAccountSource,
    replica: AccountDirectoryReplica,
}

impl ReplicatedAccountSource {
    /// Waits for a freshly synchronized snapshot containing the selected account
    /// before exposing the client. A restored local address alone is insufficient
    /// to establish that the service instance is still the published one.
    pub async fn start(
        provider_id: String,
        account_id: String,
        replica_config: AccountReplicaConfig,
        connection: AccountConnectionConfig,
    ) -> io::Result<Self> {
        let replica = AccountDirectoryReplica::start(replica_config).await?;
        let directory = replica.directory();
        let mut status = replica.subscribe();
        let mut last_observation = "account directory has not synchronized".to_owned();
        let ready = tokio::time::timeout(
            Duration::from_millis(connection.initial_sync_timeout_ms.get()),
            async {
                loop {
                    let observed = status.borrow_and_update().clone();
                    match observed {
                        AccountReplicaState::CaughtUp { .. } => {
                            if directory
                                .current(&provider_id, &account_id)?
                                .is_some_and(|entry| entry.authority.is_some())
                            {
                                return Ok::<_, io::Error>(());
                            }
                            last_observation =
                                "selected account endpoint is not published".to_owned();
                        }
                        AccountReplicaState::RetryPending(error) => last_observation = error,
                        AccountReplicaState::Stopped => {
                            return Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "account replica stopped before synchronization",
                            ));
                        }
                        AccountReplicaState::Discovering | AccountReplicaState::Following(_) => {}
                    }
                    status.changed().await.map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "account replica exited before synchronization",
                        )
                    })?;
                }
            },
        )
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("account initial synchronization timed out: {last_observation}"),
            ))
        });
        if let Err(error) = ready {
            return match replica.stop().await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(io::Error::other(format!(
                    "account startup: {error}; replica shutdown: {cleanup}"
                ))),
            };
        }
        let remote = RemoteAccountSource::new(
            provider_id,
            account_id,
            directory,
            connection.frame_bytes,
            Duration::from_millis(connection.request_timeout_ms.get()),
        )?;
        Ok(Self { remote, replica })
    }

    pub fn subscribe(&self) -> watch::Receiver<AccountReplicaState> {
        self.replica.subscribe()
    }

    pub async fn stop(self) -> io::Result<()> {
        self.replica.stop().await
    }
}

impl AccountCredentialSource for ReplicatedAccountSource {
    async fn current(&self) -> io::Result<PublishedAccount> {
        self.remote.current().await
    }

    async fn refresh(
        &self,
        previous_revision: CommitId,
        context: ExternalAuthRefreshContext,
    ) -> io::Result<PublishedAccount> {
        self.remote.refresh(previous_revision, context).await
    }
}
