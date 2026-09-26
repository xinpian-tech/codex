use std::io;
use std::path::PathBuf;

use codex_infra_protocol::MachineId;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;
use codex_infra_runtime::ArchiveController;
use codex_infra_runtime::ArchiveJob;
use codex_infra_runtime::ArchiveTarget;
use codex_infra_runtime::MachineArchiveWriter;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;
use codex_infra_state::Journal;
use codex_infra_state::JournalPosition;
use codex_infra_state::JournalReader;

use super::audit::ControlAudit;
use super::audit::InputCompletion;

pub(super) struct ControlArchive {
    pub(super) directory: PathBuf,
    pub(super) root_session_id: RootSessionId,
    pub(super) machine_id: MachineId,
    pub(super) run_id: MessageId,
}

impl ControlArchive {
    /// Persist the exact jobs before queue admission, retaining their IDs when
    /// admission or remote publication is interrupted.
    pub(super) fn prepare(
        self,
        positions: &[(&str, JournalPosition)],
    ) -> io::Result<Vec<ArchiveJob>> {
        let receipts = self.directory.join("receipts");
        std::fs::create_dir_all(&receipts)?;
        let jobs: Vec<_> = positions
            .iter()
            .map(|(name, position)| ArchiveJob {
                job_id: MessageId::new(),
                stream: ArchiveStream {
                    root_session_id: self.root_session_id,
                    machine_id: self.machine_id.clone(),
                    producer: ArchiveProducer::Machine {
                        machine_run_id: self.run_id,
                    },
                    name: format!("machine-control-{name}"),
                },
                source: self.directory.join(format!("{name}.journal")),
                receipt_journal: receipts.join(format!("{name}.journal")),
                target: ArchiveTarget::ProducerFinished(*position),
            })
            .collect();
        let mut prepared = Journal::open(&self.directory.join("archive.journal"), |_| {
            Err(io::Error::other("control archive already prepared"))
        })?;
        prepared.append(&serde_json::to_vec(&jobs)?)?;
        Ok(jobs)
    }
}

/// Re-admit one saved run at a time without loading historical journal bodies.
/// Runs without a complete prepared record remain available for recovery of
/// their producers; they are not inferred complete from file lengths.
pub(super) async fn replay_control_archives(
    directory: PathBuf,
    archive: ArchiveController,
    root_session_id: RootSessionId,
    machine_id: MachineId,
) -> io::Result<()> {
    let mut entries = tokio::task::spawn_blocking(move || std::fs::read_dir(directory))
        .await
        .map_err(io::Error::other)??;
    loop {
        let (returned, jobs) = tokio::task::spawn_blocking(move || {
            let jobs = match entries.next().transpose()? {
                None => None,
                Some(entry) => {
                    let path = entry.path().join("archive.journal");
                    let jobs = match JournalReader::open(&path, JournalPosition::default()) {
                        Ok(mut reader) => match reader.next_record()? {
                            Some(record) => {
                                serde_json::from_slice::<Vec<ArchiveJob>>(&record.payload)
                                    .map_err(io::Error::other)?
                            }
                            None => Vec::new(),
                        },
                        Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
                        Err(error) => return Err(error),
                    };
                    Some(jobs)
                }
            };
            Ok::<_, io::Error>((entries, jobs))
        })
        .await
        .map_err(io::Error::other)??;
        entries = returned;
        let Some(jobs) = jobs else {
            return Ok(());
        };
        for job in jobs {
            if job.stream.root_session_id != root_session_id || job.stream.machine_id != machine_id
            {
                return Err(io::Error::other("control archive machine/session mismatch"));
            }
            if archive.completion(job.job_id).await?.is_none() {
                archive.enqueue(job).await?;
            }
        }
    }
}

/// Runs after control output ends. Completion is established by each exact
/// job's durable remote receipt, independently of unrelated machine backlog.
/// When startup did not produce a writer, only prepares the durable jobs for
/// the next startup to admit; it does not establish remote completion.
pub(super) async fn archive_control(
    audit: ControlAudit,
    input: io::Result<InputCompletion>,
    writer: Option<MachineArchiveWriter>,
) -> io::Result<()> {
    tokio::task::spawn_blocking(move || {
        let jobs = audit.finish(&input)?;
        let Some(mut writer) = writer else {
            return input.map(|_| ());
        };
        for job in &jobs {
            writer.enqueue(job.clone())?;
        }
        loop {
            let mut complete = true;
            for job in &jobs {
                complete &= writer.completion(job.job_id)?.is_some();
            }
            if complete {
                return input.map(|_| ());
            }
            if writer.advance_one()?.is_none() {
                return Err(io::Error::other("control archive receipts missing"));
            }
        }
    })
    .await
    .map_err(io::Error::other)?
}
