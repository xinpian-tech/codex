use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_protocol::MessageId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::watch;

use crate::AccountAuthority;

/// A per-account event chain. Publishers name the preceding event, including
/// across owner handover; replicas apply missing predecessors before successors.
/// A missing authority withdraws the endpoint while retaining the chain head.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountDirectoryUpdate {
    pub event_id: MessageId,
    pub provider_id: String,
    pub account_id: String,
    pub previous_event_id: Option<MessageId>,
    pub authority: Option<AccountAuthority>,
}

type AccountKey = (String, String);

struct State {
    source: PathBuf,
    journal: Journal,
    entries: BTreeMap<AccountKey, AccountDirectoryUpdate>,
    subscribers: BTreeMap<AccountKey, watch::Sender<Option<AccountAuthority>>>,
}

/// Durable local replica of account endpoint metadata. Each account has its own
/// watch channel; unrelated directory updates never notify an Agent's client.
/// Retain the directory handle while consuming its authority subscriptions.
#[derive(Clone)]
pub struct AccountDirectory {
    state: Arc<Mutex<State>>,
}

impl AccountDirectory {
    /// Blocking journal replay. The caller creates the containing spool directory.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut entries = BTreeMap::new();
        let journal = Journal::open(path, |record| {
            let update: AccountDirectoryUpdate = serde_json::from_slice(&record.payload)?;
            let key = (update.provider_id.clone(), update.account_id.clone());
            check_update(entries.get(&key), &update)?;
            entries.insert(key, update);
            Ok(())
        })?;
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                source: path.canonicalize()?,
                journal,
                entries,
                subscribers: BTreeMap::new(),
            })),
        })
    }

    /// The producer-confirmed durable prefix, sampled under the append lock.
    /// The stream remains open for subsequent publications and withdrawals.
    pub fn archive_snapshot(&self) -> io::Result<(PathBuf, JournalPosition)> {
        let state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok((state.source.clone(), state.journal.position()))
    }

    pub fn current(
        &self,
        provider_id: &str,
        account_id: &str,
    ) -> io::Result<Option<AccountDirectoryUpdate>> {
        let state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(state
            .entries
            .get(&(provider_id.to_owned(), account_id.to_owned()))
            .cloned())
    }

    /// Subscription may precede discovery. RemoteAccountSource reports an absent
    /// endpoint until a publication arrives, and after an explicit withdrawal.
    pub fn subscribe(
        &self,
        provider_id: &str,
        account_id: &str,
    ) -> io::Result<watch::Receiver<Option<AccountAuthority>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let key = (provider_id.to_owned(), account_id.to_owned());
        let authority = state
            .entries
            .get(&key)
            .and_then(|entry| entry.authority.clone());
        Ok(state
            .subscribers
            .entry(key)
            .or_insert_with(|| watch::channel(authority).0)
            .subscribe())
    }

    /// Acknowledges only after journal persistence and watch publication. The
    /// owned blocking worker completes even if the caller cancels its waiter.
    pub async fn apply(&self, update: AccountDirectoryUpdate) -> io::Result<()> {
        let directory = self.clone();
        tokio::task::spawn_blocking(move || directory.apply_update(update))
            .await
            .map_err(io::Error::other)?
    }

    pub(super) fn apply_update(&self, update: AccountDirectoryUpdate) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let key = (update.provider_id.clone(), update.account_id.clone());
        let previous = state.entries.get(&key);
        check_update(previous, &update)?;
        if previous == Some(&update) {
            return Ok(());
        }
        state.journal.append(&serde_json::to_vec(&update)?)?;
        if let Some(subscriber) = state.subscribers.get(&key) {
            subscriber.send_replace(update.authority.clone());
        }
        state.entries.insert(key, update);
        Ok(())
    }
}

fn check_update(
    previous: Option<&AccountDirectoryUpdate>,
    update: &AccountDirectoryUpdate,
) -> io::Result<()> {
    if previous == Some(update) {
        return Ok(());
    }
    if previous.is_some_and(|previous| previous.event_id == update.event_id)
        || update.previous_event_id == Some(update.event_id)
    {
        return Err(io::Error::other("account directory event id was reused"));
    }
    if previous.map(|previous| previous.event_id) != update.previous_event_id {
        return Err(io::Error::other(
            "account directory predecessor differs from local head",
        ));
    }
    Ok(())
}
