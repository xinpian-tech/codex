use std::io;

use serde::Deserialize;
use serde::Serialize;

use crate::PaneOutput;
use crate::parse_pane_output;

const MAX_CONTROL_LINE_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub enum ControlRecord {
    PaneOutput(PaneOutput),
    Notification(Vec<u8>),
}

/// Incremental control-mode decoding, including notifications that the runtime
/// uses to track client readiness and pane lifecycle. Save this state with the
/// source journal cursor when pausing extraction between arbitrary byte chunks.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ControlDecoder {
    line: Vec<u8>,
}

impl ControlDecoder {
    pub fn feed(
        &mut self,
        bytes: &[u8],
        mut receive: impl FnMut(ControlRecord) -> io::Result<()>,
    ) -> io::Result<()> {
        for &byte in bytes {
            if byte == b'\n' {
                let line = std::mem::take(&mut self.line);
                match parse_pane_output(&line)? {
                    Some(output) => receive(ControlRecord::PaneOutput(output))?,
                    None => receive(ControlRecord::Notification(line))?,
                }
            } else {
                if self.line.len() == MAX_CONTROL_LINE_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "tmux control line exceeds decoder capacity",
                    ));
                }
                self.line.push(byte);
            }
        }
        Ok(())
    }
}
