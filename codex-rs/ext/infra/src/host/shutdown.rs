use std::io;
use std::path::PathBuf;

use super::HostArchiveJobIds;
use super::HostArchiveJobs;
use super::HostArchivePhase;
use super::ManagedHost;
use super::archive::prepare_jobs;

impl ManagedHost {
    /// Stops the embedded inference service, then drains managed hook and
    /// process work before sampling its audited prefix. The caller first stops
    /// external thread creation and arranges for long-running children to exit.
    ///
    /// Cancellation only drops the waiter; the owned shutdown task continues.
    /// Returned jobs are snapshots, not stream seals or Agent completion. The
    /// finalizer still persists/submits these jobs, waits for archive receipts,
    /// performs final publication, and records the remaining producer tails.
    pub async fn shutdown_and_snapshot(
        mut self,
        receipts: PathBuf,
        ids: HostArchiveJobIds,
    ) -> io::Result<HostArchiveJobs> {
        if !receipts.is_absolute() {
            return Err(io::Error::other(
                "archive receipt directory must be absolute",
            ));
        }
        tokio::spawn(async move {
            let rpc = self.rpc.close().await;
            let inference = self.client.shutdown_drained().await;
            let events = self.events.finish().await;
            let model_inputs = self.model_inputs.close().await;
            let provider = match self.provider.take() {
                Some(provider) => provider.stop().await,
                None => Ok(()),
            };
            let account = match self.account_replica.take() {
                Some(replica) => replica.stop().await,
                None => Ok(()),
            };
            let hooks = self.hooks.shutdown().await.map_err(io::Error::other);
            // Hook workers may issue EOF or termination. Close backend request
            // admission only after those workers have finished.
            let requests = self
                .exec_backend
                .close_recorded_requests()
                .await
                .map_err(io::Error::other);
            let producers = self
                .exec_backend
                .drain_recorded_processes()
                .await
                .map_err(io::Error::other);
            let workspace = match self.tools.workspace() {
                Ok(Some(workspace)) => workspace.drain().await,
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            // Attempt all independent drains even after an earlier failure;
            // no completion snapshot is returned unless all have succeeded.
            rpc?;
            inference?;
            let events = events?;
            model_inputs?;
            provider?;
            account?;
            hooks?;
            requests?;
            producers?;
            workspace?;
            let mut jobs = prepare_jobs(
                &self.processes,
                &self.tools,
                &self.store_audit,
                self.account_observation.as_ref(),
                &receipts,
                ids.clone(),
                HostArchivePhase::Snapshot,
            )?;
            jobs.attach_request_audits(
                &self.rpc,
                &self.model_inputs,
                events,
                &receipts,
                &ids,
                HostArchivePhase::Snapshot,
            )?;
            Ok(jobs)
        })
        .await
        .map_err(io::Error::other)?
    }
}
