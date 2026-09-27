use std::io;
use std::sync::Arc;

use codex_extension_api::ModelResponseError;
use codex_extension_api::ModelResponseInterceptor;
use codex_extension_api::ModelResponseStream;
use codex_extension_api::ResponseEvent;
use codex_infra_protocol::MessageId;
use futures::StreamExt;

use super::ModelInputAudit;
use super::ModelInputAuditEvent;

pub(super) struct ResponseAudit {
    pub audit: ModelInputAudit,
    pub attempt_id: MessageId,
}

impl ModelResponseInterceptor for ResponseAudit {
    fn intercept(self: Box<Self>, stream: ModelResponseStream) -> ModelResponseStream {
        Box::pin(futures::stream::unfold(
            Some((self, stream)),
            |state| async move {
                let (observer, mut stream) = state?;
                let item = stream.next().await;
                let attempt_id = observer.attempt_id;
                let event = match &item {
                    Some(Ok(ResponseEvent::Created { response_id })) => {
                        Some(ModelInputAuditEvent::Created {
                            attempt_id,
                            response_id: response_id.clone(),
                        })
                    }
                    Some(Ok(ResponseEvent::Completed {
                        response_id,
                        token_usage,
                        usage_metadata,
                        end_turn,
                    })) => Some(ModelInputAuditEvent::Completed {
                        attempt_id,
                        response_id: response_id.clone(),
                        token_usage: token_usage.clone(),
                        usage_metadata: usage_metadata.clone(),
                        end_turn: *end_turn,
                    }),
                    Some(Ok(ResponseEvent::ServerModel(model))) => {
                        Some(ModelInputAuditEvent::ServerModel {
                            attempt_id,
                            model: model.clone(),
                        })
                    }
                    Some(Err(error)) => Some(ModelInputAuditEvent::StreamFailed {
                        attempt_id,
                        error: error.to_string(),
                    }),
                    None => Some(ModelInputAuditEvent::StreamEnded { attempt_id }),
                    Some(Ok(
                        ResponseEvent::SafetyBuffering(_)
                        | ResponseEvent::OutputItemDone(_)
                        | ResponseEvent::OutputItemAdded(_)
                        | ResponseEvent::ModelVerifications(_)
                        | ResponseEvent::TurnModerationMetadata(_)
                        | ResponseEvent::ServerReasoningIncluded(_)
                        | ResponseEvent::OutputTextDelta(_)
                        | ResponseEvent::ToolCallInputDelta { .. }
                        | ResponseEvent::ReasoningSummaryDelta { .. }
                        | ResponseEvent::ReasoningSummaryDone { .. }
                        | ResponseEvent::ReasoningContentDelta { .. }
                        | ResponseEvent::ReasoningSummaryPartAdded { .. }
                        | ResponseEvent::RateLimits(_)
                        | ResponseEvent::ModelsEtag(_),
                    )) => None,
                };
                if let Some(event) = event {
                    let writer = Arc::clone(&observer.audit.writer);
                    let recorded = tokio::task::spawn_blocking(move || {
                        let mut writer = writer
                            .lock()
                            .map_err(|error| io::Error::other(error.to_string()))?;
                        if writer.closed {
                            return Err(io::Error::other("model response audit is closed"));
                        }
                        writer.journal.append(&serde_json::to_vec(&event)?)?;
                        Ok::<_, io::Error>(())
                    })
                    .await
                    .map_err(io::Error::other)
                    .and_then(|result| result);
                    if let Err(error) = recorded {
                        return Some((
                            Err(ModelResponseError::Stream(format!(
                                "model response audit: {error}"
                            ))),
                            None,
                        ));
                    }
                }
                item.map(|item| (item, Some((observer, stream))))
            },
        ))
    }
}
