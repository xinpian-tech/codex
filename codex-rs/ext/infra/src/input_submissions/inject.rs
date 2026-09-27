use std::io;
use std::sync::Arc;

use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadInjectItemsParams;
use codex_infra_state::InboxEntry;

use super::AgentInputSubmission;
use super::AgentInputSubmissions;
use crate::AgentMessageInput;
use crate::AgentRpcOutcome;
use crate::ManagedHost;

impl AgentInputSubmissions {
    /// Owns intent persistence and RPC dispatch through recorded completion.
    /// Repeated calls reuse the original request. An uncertain outcome remains
    /// uncertain until history reconciliation; it is never automatically resent.
    /// A successful reply can mean queued input and does not acknowledge its
    /// presentation. close waits for this operation even if its waiter cancels.
    pub async fn inject(
        &self,
        host: &ManagedHost,
        entry: &InboxEntry,
        thread_id: String,
    ) -> io::Result<(AgentInputSubmission, AgentRpcOutcome)> {
        let inputs = self.clone();
        let entry = entry.clone();
        let rpc = host.rpc.clone();
        let sender = host.client.sender();
        tokio::spawn(async move {
            let _dispatch = Arc::clone(&inputs.dispatch).lock_owned().await;
            let submission = inputs.prepare(&entry, thread_id).await?;
            let items = AgentMessageInput::from_inbox(&entry, &inputs.launch)?.response_items()?;
            let outcome = rpc
                .request(
                    sender,
                    ClientRequest::ThreadInjectItems {
                        request_id: RequestId::String(submission.request_id.to_string()),
                        params: ThreadInjectItemsParams {
                            thread_id: submission.thread_id.clone(),
                            items,
                        },
                    },
                )
                .await?;
            Ok((submission, outcome))
        })
        .await
        .map_err(io::Error::other)?
    }
}
