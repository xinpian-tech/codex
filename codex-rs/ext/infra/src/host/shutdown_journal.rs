use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use codex_infra_protocol::MessageId;
use codex_infra_runtime::ArchiveController;
use codex_infra_runtime::ArchiveTarget;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::Journal;
use serde::Deserialize;
use serde::Serialize;

use super::HostArchiveJobIds;
use super::HostArchiveJobs;
use super::HostArchiveReceipts;
use super::ManagedHost;
use crate::StoreAuditIdentity;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostShutdownPlan {
    pub identity: StoreAuditIdentity,
    pub launch_id: MessageId,
    pub receipt_directory: PathBuf,
    pub jobs: HostArchiveJobIds,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event {
    Requested { plan: HostShutdownPlan },
    Prepared { jobs: Box<HostArchiveJobs> },
    Archived { receipts: Box<HostArchiveReceipts> },
    Failed { message: String },
}

#[derive(Clone)]
struct State {
    plan: HostShutdownPlan,
    opened: bool,
    running: bool,
    jobs: Option<HostArchiveJobs>,
    receipts: Option<HostArchiveReceipts>,
    failure: Option<String>,
}

struct Writer {
    journal: Journal,
    state: State,
}

/// Persists host shutdown intent and the exact post-drain archive jobs before
/// admission to the machine actor. Archived means only the configured audit
/// snapshots are remote, not final-message delivery or Agent completion.
#[derive(Clone)]
pub struct HostShutdownJournal {
    writer: Arc<Mutex<Writer>>,
}

pub enum HostShutdownStatus {
    Requested { last_failure: Option<String> },
    Draining,
    Prepared,
    Archived,
}

impl HostShutdownJournal {
    pub async fn status(&self) -> io::Result<HostShutdownStatus> {
        self.update(|writer| {
            let state = &writer.state;
            Ok(if state.receipts.is_some() {
                HostShutdownStatus::Archived
            } else if state.jobs.is_some() {
                HostShutdownStatus::Prepared
            } else if state.running {
                HostShutdownStatus::Draining
            } else {
                HostShutdownStatus::Requested {
                    last_failure: state.failure.clone(),
                }
            })
        })
        .await
    }

    pub fn open(path: &Path, plan: HostShutdownPlan) -> io::Result<Self> {
        if !plan.receipt_directory.is_absolute() {
            return Err(io::Error::other(
                "shutdown receipt directory must be absolute",
            ));
        }
        let mut state = State {
            plan,
            opened: false,
            running: false,
            jobs: None,
            receipts: None,
            failure: None,
        };
        let mut journal = Journal::open(path, |record| {
            state.apply(serde_json::from_slice(&record.payload).map_err(io::Error::other)?)
        })?;
        if !state.opened {
            let event = Event::Requested {
                plan: state.plan.clone(),
            };
            journal.append(&serde_json::to_vec(&event).map_err(io::Error::other)?)?;
            state.apply(event)?;
        }
        Ok(Self {
            writer: Arc::new(Mutex::new(Writer { journal, state })),
        })
    }

    /// The owned worker persists its prepared snapshot even if the caller stops
    /// waiting. Recovery with prepared jobs calls archive instead of rerunning
    /// shutdown or inventing new job IDs/positions.
    pub async fn shutdown(&self, host: ManagedHost) -> io::Result<HostArchiveJobs> {
        let recorder = self.clone();
        tokio::spawn(async move {
            let identity = host.processes.identity.clone();
            let launch_id = host.processes.launch_id;
            let plan = recorder
                .update(move |writer| {
                    let state = &mut writer.state;
                    if state.running
                        || state.jobs.is_some()
                        || identity != state.plan.identity
                        || launch_id != state.plan.launch_id
                    {
                        return Err(io::Error::other(
                            "host shutdown binding or phase differs from plan",
                        ));
                    }
                    state.running = true;
                    Ok(state.plan.clone())
                })
                .await?;
            let result = host
                .shutdown_and_snapshot(plan.receipt_directory, plan.jobs)
                .await;
            recorder
                .update(move |writer| {
                    writer.state.running = false;
                    match result {
                        Ok(jobs) => {
                            writer.append(Event::Prepared {
                                jobs: Box::new(jobs.clone()),
                            })?;
                            Ok(jobs)
                        }
                        Err(error) => {
                            writer.append(Event::Failed {
                                message: error.to_string(),
                            })?;
                            Err(error)
                        }
                    }
                })
                .await
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Replays durable admission idempotently and records the complete receipt
    /// set. None means at least one job is pending; the driver can retry later.
    pub async fn archive(
        &self,
        controller: &ArchiveController,
    ) -> io::Result<Option<HostArchiveReceipts>> {
        let (jobs, previous) = self
            .update(|writer| {
                let jobs = writer.state.jobs.clone().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "host shutdown snapshot is not prepared",
                    )
                })?;
                Ok((jobs, writer.state.receipts.clone()))
            })
            .await?;
        if previous.is_some() {
            return Ok(previous);
        }
        jobs.submit(controller).await?;
        let Some(receipts) = jobs.completion(controller).await? else {
            return Ok(None);
        };
        self.update(move |writer| {
            if writer.state.receipts.as_ref() != Some(&receipts) {
                writer.append(Event::Archived {
                    receipts: Box::new(receipts.clone()),
                })?;
            }
            Ok(Some(receipts))
        })
        .await
    }

    async fn update<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Writer) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let mut writer = writer
                .lock()
                .map_err(|error| io::Error::other(error.to_string()))?;
            action(&mut writer)
        })
        .await
        .map_err(io::Error::other)?
    }
}

impl Writer {
    fn append(&mut self, event: Event) -> io::Result<()> {
        let mut next = self.state.clone();
        next.apply(event.clone())?;
        self.journal
            .append(&serde_json::to_vec(&event).map_err(io::Error::other)?)?;
        self.state = next;
        Ok(())
    }
}

impl State {
    fn apply(&mut self, event: Event) -> io::Result<()> {
        if !self.opened && !matches!(&event, Event::Requested { .. }) {
            return Err(io::Error::other("shutdown event has no recorded plan"));
        }
        match event {
            Event::Requested { plan } => {
                if self.opened || plan != self.plan {
                    return Err(io::Error::other("shutdown journal plan changed"));
                }
                self.opened = true;
            }
            Event::Prepared { jobs } => {
                if !self.opened || self.jobs.is_some() {
                    return Err(io::Error::other("shutdown snapshot phase changed"));
                }
                if jobs.account.is_some() != self.plan.jobs.account.is_some()
                    || jobs.rpc.is_some() != self.plan.jobs.rpc.is_some()
                    || jobs.model_inputs.is_some() != self.plan.jobs.model_inputs.is_some()
                {
                    return Err(io::Error::other(
                        "shutdown optional archives differ from plan",
                    ));
                }
                for (job, name, id) in [
                    (&jobs.processes, "processes", self.plan.jobs.processes),
                    (&jobs.tools, "tools", self.plan.jobs.tools),
                    (
                        &jobs.thread_store,
                        "thread-store",
                        self.plan.jobs.thread_store,
                    ),
                ]
                .into_iter()
                .chain(
                    jobs.account
                        .iter()
                        .zip(self.plan.jobs.account)
                        .map(|(job, id)| (job, "account-observations", id)),
                )
                .chain(
                    jobs.rpc
                        .iter()
                        .zip(self.plan.jobs.rpc)
                        .map(|(job, id)| (job, "rpc", id)),
                )
                .chain(
                    jobs.model_inputs
                        .iter()
                        .zip(self.plan.jobs.model_inputs)
                        .map(|(job, id)| (job, "model-inputs", id)),
                ) {
                    if job.job_id != id
                        || job.stream.name != name
                        || job.stream.root_session_id != self.plan.identity.root_session_id
                        || job.stream.producer
                            != (ArchiveProducer::Agent {
                                agent_id: self.plan.identity.agent_id,
                                launch_id: self.plan.launch_id,
                            })
                        || job.stream.machine_id != self.plan.identity.machine_id
                        || !matches!(job.target, ArchiveTarget::Snapshot(_))
                        || job.receipt_journal
                            != self
                                .plan
                                .receipt_directory
                                .join(format!("{}-{name}.journal", self.plan.launch_id))
                    {
                        return Err(io::Error::other("shutdown archive jobs differ from plan"));
                    }
                }
                self.jobs = Some(*jobs);
                self.failure = None;
            }
            Event::Archived { receipts } => {
                let receipts = *receipts;
                let expected_ref = format!(
                    "refs/codex/session-shards/{}/{}",
                    self.plan.identity.root_session_id, self.plan.identity.machine_id
                );
                if self.jobs.is_none()
                    || self.jobs.as_ref().is_some_and(|jobs| {
                        jobs.account.is_some() != receipts.account.is_some()
                            || jobs.rpc.is_some() != receipts.rpc.is_some()
                            || jobs.model_inputs.is_some() != receipts.model_inputs.is_some()
                    })
                    || self
                        .receipts
                        .as_ref()
                        .is_some_and(|previous| previous != &receipts)
                    || [&receipts.processes, &receipts.tools, &receipts.thread_store]
                        .into_iter()
                        .chain(receipts.account.iter())
                        .chain(receipts.rpc.iter())
                        .chain(receipts.model_inputs.iter())
                        .any(|receipt| receipt.session_ref != expected_ref)
                {
                    return Err(io::Error::other(
                        "shutdown archive receipt phase or binding changed",
                    ));
                }
                self.receipts = Some(receipts);
            }
            Event::Failed { message } => self.failure = Some(message),
        }
        Ok(())
    }
}
