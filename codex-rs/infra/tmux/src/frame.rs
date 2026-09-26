use std::io;
use std::io::Write;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use codex_infra_protocol::AgentMessage;
use codex_infra_protocol::FrameRoute;
use serde::Deserialize;
use serde::Serialize;

const PREFIX: &[u8] = b"\x1eCX1 ";
pub(crate) const CHUNK_BYTES: usize = 16 * 1024;
const MAX_LINE_BYTES: usize = 32 * 1024;

/// Transport metadata accompanies every chunk. The complete semantic envelope,
/// including identities, repo and pushed commit, is inside the encoded payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameChunk {
    pub route: FrameRoute,
    pub index: u32,
    pub count: u32,
    pub total_bytes: u64,
    payload: String,
}

impl FrameChunk {
    pub fn payload(&self) -> io::Result<Vec<u8>> {
        if self.count == 0 || self.index >= self.count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame chunk index",
            ));
        }
        let payload = STANDARD.decode(&self.payload).map_err(io::Error::other)?;
        let count = self.total_bytes.div_ceil(CHUNK_BYTES as u64);
        let expected = if self.index + 1 == self.count {
            self.total_bytes
                .saturating_sub(u64::from(self.index) * CHUNK_BYTES as u64)
        } else {
            CHUNK_BYTES as u64
        };
        if count != u64::from(self.count)
            || expected != payload.len() as u64
            || expected > CHUNK_BYTES as u64
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame chunk length",
            ));
        }
        Ok(payload)
    }

    /// The host serializes access to its stdout writer; the leading newline also
    /// separates a frame from preceding terminal output without a final newline.
    pub fn write(&self, writer: &mut impl Write) -> io::Result<()> {
        writer.write_all(b"\n")?;
        writer.write_all(PREFIX)?;
        serde_json::to_writer(&mut *writer, self).map_err(io::Error::other)?;
        writer.write_all(b"\n")?;
        writer.flush()
    }
}

pub fn write_message(writer: &mut impl Write, message: &AgentMessage) -> io::Result<()> {
    let bytes = serde_json::to_vec(message).map_err(io::Error::other)?;
    let count = u32::try_from(bytes.len().div_ceil(CHUNK_BYTES)).map_err(io::Error::other)?;
    let route = FrameRoute {
        message_id: message.message_id,
        root_session_id: message.root_session_id,
        from_agent_id: message.from.agent_id,
        to_agent_id: message.to.agent_id,
    };
    for (index, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
        FrameChunk {
            route: route.clone(),
            index: u32::try_from(index).map_err(io::Error::other)?,
            count,
            total_bytes: bytes.len() as u64,
            payload: STANDARD.encode(chunk),
        }
        .write(writer)?;
    }
    Ok(())
}

/// Incremental pane/stdin framing. Raw bytes are archived by the caller before
/// parsing; ordinary terminal lines remain outside the semantic transport.
#[derive(Default)]
pub struct FrameDecoder {
    line: Vec<u8>,
    discarding_line: bool,
}

impl FrameDecoder {
    pub fn feed(
        &mut self,
        bytes: &[u8],
        mut receive: impl FnMut(FrameChunk) -> io::Result<()>,
    ) -> io::Result<()> {
        for &byte in bytes {
            if byte == b'\n' {
                let mut line = std::mem::take(&mut self.line);
                let discarded = std::mem::replace(&mut self.discarding_line, false);
                if !discarded && line.starts_with(PREFIX) {
                    if line.last() == Some(&b'\r') {
                        line.pop();
                    }
                    let chunk: FrameChunk =
                        serde_json::from_slice(&line[PREFIX.len()..]).map_err(io::Error::other)?;
                    chunk.payload()?;
                    receive(chunk)?;
                }
            } else if !self.discarding_line {
                self.line.push(byte);
                if self.line.len() > MAX_LINE_BYTES {
                    self.line.clear();
                    self.discarding_line = true;
                }
            }
        }
        Ok(())
    }
}
