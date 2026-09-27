use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MessageId;
use codex_login::ExternalAuthRefreshContext;
use codex_login::ExternalAuthRefreshReason;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::AccountAction;
use crate::AccountAuthority;
use crate::AccountClientAuditConfig;
use crate::AccountClientControl;
use crate::AccountCredentialSource;
use crate::AccountDirectory;
use crate::AccountExchangeIdentity;
use crate::AccountExchangeObserver;
use crate::AccountRequest;
use crate::AccountResponse;
use crate::AccountResult;
use crate::PublishedAccount;
use crate::wire_audit::WireAudit;

/// An Agent's account client. Directory updates replace the watch value, so
/// each new request uses the current owner and dynamic port. Each exchange has
/// its own TCP connection and audit. Owned exchanges finish recording when their
/// waiter is canceled; the client control waits for them during host shutdown.
pub struct RemoteAccountSource {
    provider_id: String,
    account_id: String,
    authority: watch::Receiver<Option<AccountAuthority>>,
    _directory: AccountDirectory,
    frame_bytes: NonZeroUsize,
    timeout: Duration,
    audit: AccountClientAuditConfig,
    control: AccountClientControl,
}

impl RemoteAccountSource {
    pub fn new(
        provider_id: String,
        account_id: String,
        directory: AccountDirectory,
        frame_bytes: NonZeroUsize,
        timeout: Duration,
        audit: AccountClientAuditConfig,
    ) -> io::Result<Self> {
        if timeout.is_zero() {
            return Err(io::Error::other("account request timeout must be positive"));
        }
        let authority = directory.subscribe(&provider_id, &account_id)?;
        Ok(Self {
            provider_id,
            account_id,
            authority,
            _directory: directory,
            frame_bytes,
            timeout,
            audit,
            control: AccountClientControl::default(),
        })
    }

    pub fn control(&self) -> AccountClientControl {
        self.control.clone()
    }

    async fn request(&self, action: AccountAction) -> io::Result<PublishedAccount> {
        let authority = self.authority.borrow().clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "account endpoint is not published",
            )
        })?;
        let request_id = MessageId::new();
        let request = AccountRequest {
            request_id,
            authority: authority.clone(),
            provider_id: self.provider_id.clone(),
            account_id: self.account_id.clone(),
            action,
        };
        let audit_config = self.audit.clone();
        let frame_bytes = self.frame_bytes;
        let timeout = self.timeout;
        let active = self.control.begin().await?;
        tokio::spawn(async move {
            let _active = active;
            let audit = WireAudit::open(
                audit_config.directory,
                AccountExchangeIdentity {
                    connection_id: request_id,
                    root_session_id: audit_config.root_session_id,
                    authority: authority.clone(),
                    provider_id: request.provider_id.clone(),
                    account_id: request.account_id.clone(),
                    peer: authority.endpoint,
                    observer: AccountExchangeObserver::Agent {
                        machine_id: audit_config.machine_id,
                        agent_id: audit_config.agent_id,
                        launch_id: audit_config.launch_id,
                    },
                },
            )
            .await?;
            let result = async {
                let bytes = serde_json::to_vec(&request)?;
                if bytes.len() > frame_bytes.get() {
                    return Err(io::Error::other(
                        "account request exceeds configured frame budget",
                    ));
                }
                let length = u32::try_from(bytes.len()).map_err(io::Error::other)?;
                let deadline = tokio::time::Instant::now() + timeout;
                let mut stream =
                    tokio::time::timeout_at(deadline, TcpStream::connect(authority.endpoint))
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "account connect timed out")
                        })??;
                audit
                    .write_all(&mut stream, &length.to_be_bytes(), deadline)
                    .await?;
                audit.write_all(&mut stream, &bytes, deadline).await?;
                tokio::time::timeout_at(deadline, stream.shutdown())
                    .await
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "account request shutdown timed out",
                        )
                    })??;
                let mut header = [0; 4];
                audit.read_exact(&mut stream, &mut header, deadline).await?;
                let length =
                    usize::try_from(u32::from_be_bytes(header)).map_err(io::Error::other)?;
                if length > frame_bytes.get() {
                    return Err(io::Error::other(
                        "account response exceeds configured frame budget",
                    ));
                }
                let mut bytes = vec![0; length];
                audit.read_exact(&mut stream, &mut bytes, deadline).await?;
                let response: AccountResponse = serde_json::from_slice(&bytes)?;
                if response.request_id != request_id || response.authority != authority {
                    return Err(io::Error::other(
                        "account response differs from requested owner",
                    ));
                }
                match response.result {
                    AccountResult::Published { account } => {
                        if account.provider_id != request.provider_id
                            || account.account_id != request.account_id
                        {
                            return Err(io::Error::other(
                                "account response differs from requested account",
                            ));
                        }
                        Ok(*account)
                    }
                    AccountResult::Failed { message } => Err(io::Error::other(message)),
                }
            }
            .await;
            audit.finish(&result).await?;
            result
        })
        .await
        .map_err(io::Error::other)?
    }
}

impl AccountCredentialSource for RemoteAccountSource {
    async fn current(&self) -> io::Result<PublishedAccount> {
        self.request(AccountAction::Current).await
    }

    async fn refresh(
        &self,
        previous_revision: CommitId,
        context: ExternalAuthRefreshContext,
    ) -> io::Result<PublishedAccount> {
        match context.reason {
            ExternalAuthRefreshReason::Unauthorized => {
                self.request(AccountAction::Refresh {
                    previous_revision,
                    previous_chatgpt_account_id: context.previous_account_id,
                })
                .await
            }
        }
    }
}
