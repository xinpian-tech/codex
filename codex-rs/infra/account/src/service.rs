use std::io;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use codex_infra_protocol::CommitId;
use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_login::ExternalAuthRefreshContext;
use codex_login::ExternalAuthRefreshReason;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

use crate::AccountAction;
use crate::AccountAuthority;
use crate::AccountCredentialSource;
use crate::AccountRequest;
use crate::AccountResponse;
use crate::AccountResult;

/// One account's control endpoint, owned by the machine runtime. bind_ip is
/// the reachable machine address; the operating system allocates its port.
pub struct AccountServiceConfig {
    pub machine_id: MachineId,
    pub owner_revision: CommitId,
    pub bind_ip: IpAddr,
    pub provider_id: String,
    pub account_id: String,
    pub concurrent_requests: NonZeroUsize,
    pub frame_bytes: NonZeroUsize,
    pub io_timeout: Duration,
}

#[derive(Clone, Default)]
pub struct AccountServiceState {
    pub active_requests: usize,
    pub completed_requests: u64,
    pub failed_requests: u64,
    pub last_error: Option<String>,
}

pub struct AccountService {
    authority: AccountAuthority,
    state: watch::Receiver<AccountServiceState>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl AccountService {
    pub async fn start<S: AccountCredentialSource + 'static>(
        config: AccountServiceConfig,
        source: S,
    ) -> io::Result<Self> {
        if config.io_timeout.is_zero() {
            return Err(io::Error::other(
                "account service I/O timeout must be positive",
            ));
        }
        let listener = TcpListener::bind((config.bind_ip, 0)).await?;
        let authority = AccountAuthority {
            instance_id: MessageId::new(),
            machine_id: config.machine_id.clone(),
            owner_revision: config.owner_revision.clone(),
            endpoint: listener.local_addr()?,
        };
        let serving_authority = authority.clone();
        let source = Arc::new(source);
        let (stop, mut stopped) = oneshot::channel();
        let (state_tx, state) = watch::channel(AccountServiceState::default());
        let task = tokio::spawn(async move {
            let config = Arc::new(config);
            let mut jobs = JoinSet::new();
            let mut failure = None;
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    result = jobs.join_next(), if !jobs.is_empty() => {
                        observe(result, &state_tx, &mut failure);
                    }
                    accepted = listener.accept(), if jobs.len() < config.concurrent_requests.get() => {
                        let (stream, _) = match accepted {
                            Ok(connection) => connection,
                            Err(error) => { failure = Some(error); break; }
                        };
                        let config = Arc::clone(&config);
                        let source = Arc::clone(&source);
                        let authority = serving_authority.clone();
                        state_tx.send_modify(|state| state.active_requests += 1);
                        jobs.spawn(async move { exchange(stream, &config, authority, source.as_ref()).await });
                    }
                }
            }
            drop(listener);
            // Backend operations survive client disconnects and shutdown. The
            // source owns refresh/publish completion before a response exists.
            while let Some(result) = jobs.join_next().await {
                observe(Some(result), &state_tx, &mut failure);
            }
            match failure {
                Some(error) => Err(error),
                None => Ok(()),
            }
        });
        Ok(Self {
            authority,
            state,
            stop: Some(stop),
            task: Some(task),
        })
    }

    pub fn authority(&self) -> &AccountAuthority {
        &self.authority
    }

    pub fn subscribe(&self) -> watch::Receiver<AccountServiceState> {
        self.state.clone()
    }

    pub async fn stop(mut self) -> io::Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        match self.task.take() {
            Some(task) => task.await.map_err(io::Error::other)?,
            None => Ok(()),
        }
    }
}

impl Drop for AccountService {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

fn observe(
    result: Option<Result<io::Result<()>, tokio::task::JoinError>>,
    state: &watch::Sender<AccountServiceState>,
    failure: &mut Option<io::Error>,
) {
    let Some(result) = result else {
        return;
    };
    let error = match result {
        Ok(result) => result.err(),
        Err(error) => {
            *failure = Some(io::Error::other(error.to_string()));
            Some(io::Error::other(error))
        }
    };
    state.send_modify(|state| {
        state.active_requests -= 1;
        match error {
            Some(error) => {
                state.failed_requests = state.failed_requests.saturating_add(1);
                state.last_error = Some(error.to_string());
            }
            None => state.completed_requests = state.completed_requests.saturating_add(1),
        }
    });
}

async fn exchange<S: AccountCredentialSource>(
    mut stream: TcpStream,
    config: &AccountServiceConfig,
    authority: AccountAuthority,
    source: &S,
) -> io::Result<()> {
    let request: AccountRequest = tokio::time::timeout(config.io_timeout, async {
        let length = usize::try_from(stream.read_u32().await?).map_err(io::Error::other)?;
        if length > config.frame_bytes.get() {
            return Err(io::Error::other(
                "account request exceeds configured frame budget",
            ));
        }
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "account request read timed out"))??;
    let result = if request.authority != authority
        || request.provider_id != config.provider_id
        || request.account_id != config.account_id
    {
        Err(io::Error::other(
            "account request belongs to a different authority",
        ))
    } else {
        match request.action {
            AccountAction::Current => source.current().await,
            AccountAction::Refresh {
                previous_revision,
                previous_chatgpt_account_id,
            } => {
                source
                    .refresh(
                        previous_revision,
                        ExternalAuthRefreshContext {
                            reason: ExternalAuthRefreshReason::Unauthorized,
                            previous_account_id: previous_chatgpt_account_id,
                        },
                    )
                    .await
            }
        }
    };
    let result = result.and_then(|account| {
        if account.provider_id != config.provider_id || account.account_id != config.account_id {
            Err(io::Error::other(
                "account source returned a different account",
            ))
        } else {
            Ok(account)
        }
    });
    let backend_error = result.as_ref().err().map(ToString::to_string);
    let response = AccountResponse {
        request_id: request.request_id,
        authority,
        result: match result {
            Ok(account) => AccountResult::Published {
                account: Box::new(account),
            },
            Err(error) => AccountResult::Failed {
                message: error.to_string(),
            },
        },
    };
    let bytes = serde_json::to_vec(&response)?;
    if bytes.len() > config.frame_bytes.get() {
        return Err(io::Error::other(
            "account response exceeds configured frame budget",
        ));
    }
    let length = u32::try_from(bytes.len()).map_err(io::Error::other)?;
    tokio::time::timeout(config.io_timeout, async {
        stream.write_u32(length).await?;
        stream.write_all(&bytes).await?;
        stream.shutdown().await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "account response write timed out"))??;
    match backend_error {
        Some(error) => Err(io::Error::other(error)),
        None => Ok(()),
    }
}
