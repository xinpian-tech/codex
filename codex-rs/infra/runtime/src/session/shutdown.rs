use std::io;
use std::time::Duration;

use super::TransportSession;
use super::record;
use crate::CollectorFinished;

impl TransportSession {
    /// Called by the machine owner after the Agents have finished the output it
    /// needs to capture, and after SessionActor has returned transport ownership.
    /// Stops network intake, drains existing forwarding/input workers, then
    /// detaches the current observer and waits for its durable completion.
    /// Agent panes and tmux sessions remain alive. Durable delivery backlogs and
    /// raw collector tails remain available for subsequent replay/archival.
    pub async fn stop_capture(&mut self, poll_interval: Duration) -> io::Result<CollectorFinished> {
        if poll_interval.is_zero() {
            return Err(io::Error::other(
                "capture shutdown interval must be positive",
            ));
        }
        self.stop_network().await?;
        self.drain_in_flight(poll_interval).await?;
        record(
            &mut self.observations,
            "collector_stop_requested",
            &self.current_attachment.to_string(),
            "network stopped; in-flight workers drained".to_owned(),
        )?;
        let result = self.collector.stop().await;
        record(
            &mut self.observations,
            "collector_stop_result",
            &self.current_attachment.to_string(),
            format!("{result:?}"),
        )?;
        result
    }

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
