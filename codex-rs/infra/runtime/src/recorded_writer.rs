use std::io;
use std::io::Write;

use codex_infra_state::Journal;

/// Records intended output before terminal delivery. After a write error the
/// host reopens its transport and replays the durable outbox with unchanged IDs.
pub(crate) struct RecordedWriter<W> {
    pub(crate) journal: Journal,
    pub(crate) writer: W,
    pub(crate) failed: bool,
}

impl<W: Write> Write for RecordedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other(
                "reopen terminal output after write failure",
            ));
        }
        self.failed = true;
        self.journal.append(bytes)?;
        self.writer.write_all(bytes)?;
        self.failed = false;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "reopen terminal output after write failure",
            ));
        }
        self.failed = true;
        self.writer.flush()?;
        self.failed = false;
        Ok(())
    }
}
