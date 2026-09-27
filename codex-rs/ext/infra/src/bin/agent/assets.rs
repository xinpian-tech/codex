use std::fs;
use std::io;
use std::io::Read;
use std::path::PathBuf;

use codex_infra_protocol::MessageId;
use codex_infra_runtime::ArchiveJob;
use codex_infra_runtime::ArchiveTarget;
use codex_infra_runtime::LaunchIntent;
use codex_infra_state::ArchiveProducer;
use codex_infra_state::ArchiveStream;
use codex_infra_state::Journal;
use serde_json::json;

pub fn capture(
    home: PathBuf,
    generation: PathBuf,
    directory: PathBuf,
    launch: LaunchIntent,
    receipts: PathBuf,
) -> io::Result<ArchiveJob> {
    let source = directory.join("home-generation.journal");
    let mut journal = Journal::open(&source, |_| Ok(()))?;
    journal.append(&serde_json::to_vec(
        &json!({"kind":"snapshot","launch":launch}),
    )?)?;
    let mut pending = vec![
        (home, PathBuf::from("home")),
        (generation, PathBuf::from("generation")),
    ];
    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        if entry.path() != source && entry.file_type()?.is_file() {
            pending.push((
                entry.path(),
                PathBuf::from("launch").join(entry.file_name()),
            ));
        }
    }
    if let Some(parent) = directory.parent() {
        pending.push((
            parent.join("checkpoints.journal"),
            PathBuf::from("agent/checkpoints.journal"),
        ));
    }
    while let Some((path, relative)) = pending.pop() {
        let metadata = fs::metadata(&path)?;
        if metadata.is_dir() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                pending.push((entry.path(), relative.join(entry.file_name())));
            }
        } else if metadata.is_file() {
            use std::os::unix::fs::PermissionsExt;
            journal.append(&serde_json::to_vec(&json!({"kind":"file","path":relative,"size":metadata.len(),"mode":metadata.permissions().mode()}))?)?;
            let mut file = fs::File::open(path)?;
            let mut buffer = [0_u8; 65536];
            let mut offset = 0_u64;
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                journal.append(&serde_json::to_vec(&json!({"kind":"data","path":relative,"offset":offset,"bytes":&buffer[..count]}))?)?;
                offset += count as u64;
            }
        }
    }
    Ok(ArchiveJob {
        job_id: MessageId::new(),
        source,
        stream: ArchiveStream {
            root_session_id: launch.workspace.root_session_id,
            machine_id: launch.machine_id,
            producer: ArchiveProducer::Agent {
                agent_id: launch.workspace.agent_id,
                launch_id: launch.launch_id,
            },
            name: "home-generation".to_owned(),
        },
        receipt_journal: receipts.join(format!("{}-home-generation.journal", launch.launch_id)),
        target: ArchiveTarget::ProducerFinished(journal.position()),
    })
}
