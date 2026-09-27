use std::fs;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::Instant;

use crate::AccountAuthority;

#[derive(Clone)]
pub struct AccountServiceAuditConfig {
    pub directory: PathBuf,
    pub root_session_id: RootSessionId,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AccountExchangeIdentity {
    pub connection_id: MessageId,
    pub root_session_id: RootSessionId,
    pub authority: AccountAuthority,
    pub provider_id: String,
    pub account_id: String,
    pub peer: SocketAddr,
}

#[derive(Serialize, Deserialize)]
pub struct AccountExchangeFinished {
    pub identity: AccountExchangeIdentity,
    pub position: JournalPosition,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event<'a> {
    Opened {
        identity: &'a AccountExchangeIdentity,
    },
    Received {
        bytes: &'a [u8],
    },
    SendPlanned {
        bytes: &'a [u8],
    },
    Sent {
        length: usize,
    },
    Finished {
        error: Option<String>,
    },
}

pub(crate) struct WireAudit {
    directory: PathBuf,
    identity: AccountExchangeIdentity,
    journal: Arc<Mutex<Journal>>,
}

impl WireAudit {
    pub(crate) async fn open(
        directory: PathBuf,
        identity: AccountExchangeIdentity,
    ) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let directory = directory.join(identity.connection_id.to_string());
            fs::create_dir_all(&directory)?;
            let directory = directory.canonicalize()?;
            let mut journal = Journal::open(&directory.join("io.journal"), |_| {
                Err(io::Error::other(
                    "account connection id already has an audit",
                ))
            })?;
            journal.append(&serde_json::to_vec(&Event::Opened {
                identity: &identity,
            })?)?;
            #[cfg(unix)]
            {
                fs::File::open(&directory)?.sync_all()?;
                if let Some(parent) = directory.parent() {
                    fs::File::open(parent)?.sync_all()?;
                }
            }
            Ok(Self {
                directory,
                identity,
                journal: Arc::new(Mutex::new(journal)),
            })
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn record(&self, event: Event<'_>) -> io::Result<()> {
        let bytes = serde_json::to_vec(&event)?;
        let journal = Arc::clone(&self.journal);
        tokio::task::spawn_blocking(move || {
            journal
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?
                .append(&bytes)?;
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(crate) async fn read_exact(
        &self,
        stream: &mut TcpStream,
        bytes: &mut [u8],
        deadline: Instant,
    ) -> io::Result<()> {
        let mut offset = 0;
        while offset < bytes.len() {
            let end = bytes.len().min(offset + 8192);
            let count = tokio::time::timeout_at(deadline, stream.read(&mut bytes[offset..end]))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "account request read timed out")
                })??;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "account request ended before its frame",
                ));
            }
            self.record(Event::Received {
                bytes: &bytes[offset..offset + count],
            })
            .await?;
            offset += count;
        }
        Ok(())
    }

    pub(crate) async fn write_all(
        &self,
        stream: &mut TcpStream,
        bytes: &[u8],
        deadline: Instant,
    ) -> io::Result<()> {
        for chunk in bytes.chunks(8192) {
            self.record(Event::SendPlanned { bytes: chunk }).await?;
            let mut offset = 0;
            while offset < chunk.len() {
                let count = tokio::time::timeout_at(deadline, stream.write(&chunk[offset..]))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "account response write timed out")
                    })??;
                if count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "account response write returned zero",
                    ));
                }
                self.record(Event::Sent { length: count }).await?;
                offset += count;
            }
        }
        Ok(())
    }

    pub(crate) async fn finish(self, result: &io::Result<()>) -> io::Result<()> {
        self.record(Event::Finished {
            error: result.as_ref().err().map(ToString::to_string),
        })
        .await?;
        tokio::task::spawn_blocking(move || {
            let position = self
                .journal
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?
                .position();
            let finished = AccountExchangeFinished {
                identity: self.identity,
                position,
            };
            let pending = self.directory.join("finished.pending");
            let mut file = fs::File::create(&pending)?;
            file.write_all(&serde_json::to_vec(&finished)?)?;
            file.sync_all()?;
            fs::rename(pending, self.directory.join("finished.json"))?;
            #[cfg(unix)]
            fs::File::open(&self.directory)?.sync_all()?;
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }
}
