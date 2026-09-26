use std::io;

#[derive(Debug)]
pub struct PaneOutput {
    pub pane_id: String,
    pub bytes: Vec<u8>,
}

/// Decodes a control-mode `%output` notification after its terminating newline
/// has been removed. Other control notifications belong to lifecycle handling.
pub fn parse_pane_output(line: &[u8]) -> io::Result<Option<PaneOutput>> {
    let Some(output) = line.strip_prefix(b"%output ") else {
        return Ok(None);
    };
    let separator = output
        .iter()
        .position(|byte| *byte == b' ')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "tmux pane separator"))?;
    let pane_id = std::str::from_utf8(&output[..separator])
        .map_err(io::Error::other)?
        .to_owned();
    let escaped = &output[separator + 1..];
    let mut bytes = Vec::with_capacity(escaped.len());
    let mut index = 0;
    while index < escaped.len() {
        if escaped[index] == b'\\' {
            let octal = escaped.get(index + 1..index + 4).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "incomplete tmux octal escape")
            })?;
            if !(b'0'..=b'3').contains(&octal[0])
                || !octal[1..].iter().all(|byte| (b'0'..=b'7').contains(byte))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid tmux octal escape",
                ));
            }
            bytes.push((octal[0] - b'0') * 64 + (octal[1] - b'0') * 8 + (octal[2] - b'0'));
            index += 4;
        } else {
            bytes.push(escaped[index]);
            index += 1;
        }
    }
    Ok(Some(PaneOutput { pane_id, bytes }))
}
