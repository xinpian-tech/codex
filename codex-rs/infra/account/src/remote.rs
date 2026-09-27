use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MessageId;
use codex_login::ExternalAuthRefreshContext;
use codex_login::ExternalAuthRefreshReason;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::AccountAction;
use crate::AccountAuthority;
use crate::AccountCredentialSource;
use crate::AccountRequest;
use crate::AccountResponse;
use crate::AccountResult;
use crate::PublishedAccount;

/// An Agent's account client. Directory updates replace the watch value, so
/// each new request uses the current owner and dynamic port. Each exchange has
/// its own TCP connection; cancellation closes that exchange without replaying
/// a partial packet. The owner deduplicates refresh by credential revision.
pub struct RemoteAccountSource {
    provider_id: String,
    account_id: String,
    authority: watch::Receiver<AccountAuthority>,
    frame_bytes: NonZeroUsize,
    timeout: Duration,
}

impl RemoteAccountSource {
    pub fn new(
        provider_id: String,
        account_id: String,
        authority: watch::Receiver<AccountAuthority>,
        frame_bytes: NonZeroUsize,
        timeout: Duration,
    ) -> io::Result<Self> {
        if timeout.is_zero() {
            return Err(io::Error::other("account request timeout must be positive"));
        }
        Ok(Self {
            provider_id,
            account_id,
            authority,
            frame_bytes,
            timeout,
        })
    }

    async fn request(&self, action: AccountAction) -> io::Result<PublishedAccount> {
        let authority = self.authority.borrow().clone();
        let request_id = MessageId::new();
        let request = AccountRequest {
            request_id,
            authority: authority.clone(),
            provider_id: self.provider_id.clone(),
            account_id: self.account_id.clone(),
            action,
        };
        let bytes = serde_json::to_vec(&request)?;
        if bytes.len() > self.frame_bytes.get() {
            return Err(io::Error::other(
                "account request exceeds configured frame budget",
            ));
        }
        let length = u32::try_from(bytes.len()).map_err(io::Error::other)?;
        tokio::time::timeout(self.timeout, async {
            let mut stream = TcpStream::connect(authority.endpoint).await?;
            stream.write_u32(length).await?;
            stream.write_all(&bytes).await?;
            stream.shutdown().await?;
            let length = usize::try_from(stream.read_u32().await?).map_err(io::Error::other)?;
            if length > self.frame_bytes.get() {
                return Err(io::Error::other(
                    "account response exceeds configured frame budget",
                ));
            }
            let mut bytes = vec![0; length];
            stream.read_exact(&mut bytes).await?;
            let response: AccountResponse = serde_json::from_slice(&bytes)?;
            if response.request_id != request_id || response.authority != authority {
                return Err(io::Error::other(
                    "account response differs from requested owner",
                ));
            }
            match response.result {
                AccountResult::Published { account } => {
                    if account.provider_id != self.provider_id
                        || account.account_id != self.account_id
                    {
                        return Err(io::Error::other(
                            "account response differs from requested account",
                        ));
                    }
                    Ok(*account)
                }
                AccountResult::Failed { message } => Err(io::Error::other(message)),
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "account request timed out"))?
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
