use std::future::Future;
use std::io;
use std::num::NonZeroUsize;

use tokio_util::sync::CancellationToken;

use super::AgentEventCursor;
use super::AgentServerEvent;
use super::AgentServerEventRecord;
use super::AgentServerEvents;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentEventConsumerExit {
    Stopped,
    SourceClosed,
}

impl AgentServerEvents {
    /// Drives bounded pages independently of capture. The handler must finish
    /// or durably hand off each event before returning success. Retried pages
    /// can repeat handlers after failure, so effects must use event positions
    /// as identities (reply already does this).
    ///
    /// Cooperative stop finishes the current page before acknowledging it. It
    /// does not cancel handlers midway, stop inference, or close the cursor.
    /// A handler error leaves the page pending for explicit recovery. The owner
    /// must drive this future to an outcome rather than dropping it to stop.
    pub async fn consume<F, H>(
        &self,
        cursor: &AgentEventCursor,
        page_size: NonZeroUsize,
        stop: CancellationToken,
        mut handler: H,
    ) -> io::Result<AgentEventConsumerExit>
    where
        H: FnMut(AgentServerEventRecord) -> F,
        F: Future<Output = io::Result<()>>,
    {
        let mut updates = self.subscribe();
        loop {
            if stop.is_cancelled() {
                return Ok(AgentEventConsumerExit::Stopped);
            }
            // Mark notification state before sampling the journal, so a write
            // arriving during the scan remains visible to changed().
            updates
                .borrow_and_update()
                .clone()
                .map_err(io::Error::other)?;
            let page = cursor.read_page(self, page_size).await?;
            if !page.records.is_empty() {
                let closed = page
                    .records
                    .iter()
                    .any(|record| matches!(record.event, AgentServerEvent::Closed));
                for record in page.records.iter().cloned() {
                    handler(record).await?;
                }
                cursor.acknowledge(&page).await?;
                if closed {
                    return Ok(AgentEventConsumerExit::SourceClosed);
                }
                if page.next != page.durable_end {
                    continue;
                }
            }
            tokio::select! {
                _ = stop.cancelled() => return Ok(AgentEventConsumerExit::Stopped),
                changed = updates.changed() => {
                    changed.map_err(|_| io::Error::other("event capture ended without a pending Closed event"))?;
                }
            }
        }
    }
}
