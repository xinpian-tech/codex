use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::PathBuf;

use codex_infra_protocol::AgentId;
use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::MessageId;
use codex_infra_protocol::RootSessionId;

use crate::FrameChunk;
use crate::FrameRoute;
use crate::frame::CHUNK_BYTES;

struct Assembly {
    file: File,
    route: FrameRoute,
    count: u32,
    total_bytes: u64,
    received: BTreeSet<u32>,
}

/// Per-host scratch assembly of frames from its own tmux stdin. The sender keeps
/// its durable outbound record until the receiving host's inbox has accepted the
/// complete envelope, so interrupted partial assemblies can be replayed.
pub struct MessageAssembler {
    directory: PathBuf,
    root_session_id: RootSessionId,
    agent_id: AgentId,
    pending: BTreeMap<MessageId, Assembly>,
}

impl MessageAssembler {
    pub fn open(
        directory: PathBuf,
        root_session_id: RootSessionId,
        agent_id: AgentId,
    ) -> io::Result<Self> {
        fs::create_dir_all(&directory)?;
        Ok(Self {
            directory,
            root_session_id,
            agent_id,
            pending: BTreeMap::new(),
        })
    }

    pub fn accept(&mut self, chunk: FrameChunk) -> io::Result<Option<AgentMessage>> {
        if chunk.route.root_session_id != self.root_session_id
            || chunk.route.to_agent_id != self.agent_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame is addressed to another Agent",
            ));
        }
        let bytes = chunk.payload()?;
        let message_id = chunk.route.message_id;
        let path = self.directory.join(format!("{message_id}.partial"));
        let assembly = match self.pending.entry(message_id) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)?;
                entry.insert(Assembly {
                    file,
                    route: chunk.route.clone(),
                    count: chunk.count,
                    total_bytes: chunk.total_bytes,
                    received: BTreeSet::new(),
                })
            }
        };
        if assembly.route != chunk.route
            || assembly.count != chunk.count
            || assembly.total_bytes != chunk.total_bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message chunks disagree on their envelope",
            ));
        }
        let offset = u64::from(chunk.index) * CHUNK_BYTES as u64;
        assembly.file.seek(SeekFrom::Start(offset))?;
        if assembly.received.contains(&chunk.index) {
            let mut previous = vec![0; bytes.len()];
            assembly.file.read_exact(&mut previous)?;
            if previous != bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "replayed chunk has different bytes",
                ));
            }
        } else {
            assembly.file.write_all(&bytes)?;
            assembly.received.insert(chunk.index);
        }
        if assembly.received.len() != chunk.count as usize {
            return Ok(None);
        }
        assembly.file.seek(SeekFrom::Start(0))?;
        let message: AgentMessage = serde_json::from_reader(BufReader::new(&mut assembly.file))
            .map_err(io::Error::other)?;
        if message.message_id != message_id
            || message.root_session_id != self.root_session_id
            || message.from.agent_id != chunk.route.from_agent_id
            || message.to.agent_id != self.agent_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message envelope differs from its frame route",
            ));
        }
        self.pending.remove(&message_id);
        fs::remove_file(path)?;
        Ok(Some(message))
    }
}
