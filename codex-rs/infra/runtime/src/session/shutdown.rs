use std::io;

use super::TransportSession;

impl TransportSession {
    /// Run after taking ownership back from SessionActor. Stops new network
    /// reads, journals every decoded event handed off by the readers and stages
    /// recorded frames into the durable inbox. This does not wait for Agent
    /// delivery, stop collectors, or declare the Root Session finished.
    ///
    /// Cancellation retains the event already removed from the receiver, and
    /// calling again continues the drain. Once requested, reads stay stopped.
    pub async fn stop_network(&mut self) -> io::Result<()> {
        self.reception.stop_reading();
        loop {
            self.record_pending_reception()?;
            let Some(event) = self.reception.next_event().await else {
                break;
            };
            self.pending_reception = Some(event);
        }
        while self.ingress.advance_one(&mut self.inbox)?.is_some() {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    pub(super) fn record_pending_reception(&mut self) -> io::Result<()> {
        if let Some(event) = &self.pending_reception {
            self.ingress.record(event)?;
            self.pending_reception = None;
        }
        Ok(())
    }
}
