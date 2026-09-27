use std::fs;
use std::io;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use codex_infra_protocol::CommitId;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;
use codex_infra_state::JournalPosition;
use codex_infra_state::SessionShard;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::AccountDirectory;
use crate::AccountDirectoryFollower;
use crate::AccountDirectoryPage;
use crate::AccountDirectoryProgress;
use crate::AccountReplicaControl;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountReplicaConfig {
    pub git: PathBuf,
    pub repository: PathBuf,
    pub remote: String,
    pub directory: PathBuf,
    pub stream: ArchiveStream,
    pub page_size: NonZeroUsize,
    pub batch_size: NonZeroUsize,
    pub poll_interval_ms: NonZeroU64,
}

#[derive(Clone, Debug)]
pub enum AccountReplicaState {
    Discovering,
    Following(AccountDirectoryProgress),
    CaughtUp {
        revision: CommitId,
        position: JournalPosition,
    },
    RetryPending(String),
    Stopped,
}

/// One Agent's account directory replica. Git discovery and journal restoration
/// run off the async executor; each tick reads one catalog page or applies one
/// bounded batch. Agent semantic messages are not carried by this service.
pub struct AccountDirectoryReplica {
    directory: AccountDirectory,
    control: AccountReplicaControl,
    state: watch::Receiver<AccountReplicaState>,
}

struct Worker {
    config: AccountReplicaConfig,
    shard: SessionShard,
    directory: AccountDirectory,
    discovery: Option<(CommitId, Option<String>)>,
    restored: Option<(CommitId, JournalPosition)>,
    follower: Option<AccountDirectoryFollower>,
    caught_up: bool,
}

impl AccountDirectoryReplica {
    pub async fn start(mut config: AccountReplicaConfig) -> io::Result<Self> {
        let interval = Duration::from_millis(config.poll_interval_ms.get());
        let mut worker = tokio::task::spawn_blocking(move || {
            if config.stream.name != "account-directory"
                || !matches!(config.stream.producer, ArchiveProducer::Machine { .. })
            {
                return Err(io::Error::other(
                    "account replica requires a machine directory stream",
                ));
            }
            fs::create_dir_all(&config.directory)?;
            config.directory = config.directory.canonicalize()?;
            let directory = AccountDirectory::open(&config.directory.join("replica.journal"))?;
            let shard = SessionShard::open(
                config.git.clone(),
                config.repository.canonicalize()?,
                config.directory.join("read-index"),
                config.remote.clone(),
                config.stream.root_session_id,
                &config.stream.machine_id,
            )?;
            Ok::<_, io::Error>(Worker {
                config,
                shard,
                directory,
                discovery: None,
                restored: None,
                follower: None,
                caught_up: false,
            })
        })
        .await
        .map_err(io::Error::other)??;
        let directory = worker.directory.clone();
        let (stop, mut stopped) = oneshot::channel();
        let (status, state) = watch::channel(AccountReplicaState::Discovering);
        let task = tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    _ = timer.tick() => {}
                }
                let (returned, result) = tokio::task::spawn_blocking(move || {
                    let result = worker.step();
                    (worker, result)
                })
                .await
                .map_err(io::Error::other)?;
                worker = returned;
                status.send_replace(match result {
                    Ok(state) => state,
                    Err(error) => AccountReplicaState::RetryPending(error.to_string()),
                });
            }
            status.send_replace(AccountReplicaState::Stopped);
            Ok::<_, io::Error>(())
        });
        let (finished, completion) = watch::channel(None);
        tokio::spawn(async move {
            let result = match task.await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(error) => Err(error.to_string()),
            };
            finished.send_replace(Some(result));
        });
        Ok(Self {
            directory,
            control: AccountReplicaControl {
                stop: Arc::new(Mutex::new(Some(stop))),
                completion,
            },
            state,
        })
    }

    pub fn directory(&self) -> AccountDirectory {
        self.directory.clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<AccountReplicaState> {
        self.state.clone()
    }

    pub fn control(&self) -> AccountReplicaControl {
        self.control.clone()
    }

    /// Stops after the active Git/restore/apply step. Dropping this owner or
    /// canceling the stop waiter also leaves the owned step running to completion.
    pub async fn stop(self) -> io::Result<()> {
        self.control.stop().await
    }
}

impl Drop for AccountDirectoryReplica {
    fn drop(&mut self) {
        let _ = self.control.request_stop();
    }
}

impl Worker {
    fn step(&mut self) -> io::Result<AccountReplicaState> {
        if !self.caught_up
            && let (Some(follower), Some((revision, position))) = (&self.follower, &self.restored)
        {
            let progress = follower.advance_blocking(*position, self.config.batch_size)?;
            self.caught_up = progress.caught_up;
            return Ok(if progress.caught_up {
                AccountReplicaState::CaughtUp {
                    revision: revision.clone(),
                    position: *position,
                }
            } else {
                AccountReplicaState::Following(progress)
            });
        }
        if self.discovery.is_none() {
            self.discovery = Some((self.shard.fetch_archive_head()?, None));
        }
        let (revision, after) = self
            .discovery
            .as_ref()
            .ok_or_else(|| io::Error::other("account discovery revision missing"))?;
        let page = AccountDirectoryPage::read(
            &self.shard,
            revision,
            after.as_deref(),
            self.config.page_size,
        )?;
        if let Some(published) = page
            .data
            .into_iter()
            .find(|entry| entry.stream() == &self.config.stream)
        {
            if self
                .restored
                .as_ref()
                .is_some_and(|(_, position)| *position == published.position())
            {
                let result = AccountReplicaState::CaughtUp {
                    revision: revision.clone(),
                    position: published.position(),
                };
                self.discovery = None;
                return Ok(result);
            }
            let source = published.restore(
                &self.shard,
                &self.config.directory.join("source.journal"),
                self.config.page_size,
            )?;
            if self.follower.is_none() {
                self.follower = Some(AccountDirectoryFollower::open(
                    source,
                    self.directory.clone(),
                    &self.config.directory.join("cursor.journal"),
                )?);
            }
            self.restored = Some((published.revision().clone(), published.position()));
            self.caught_up = false;
            self.discovery = None;
            return Ok(AccountReplicaState::Discovering);
        }
        self.discovery = page
            .next_cursor
            .map(|after| (revision.clone(), Some(after)));
        Ok(AccountReplicaState::Discovering)
    }
}
