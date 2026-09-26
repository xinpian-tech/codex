use std::fmt::Display;
use std::io;
use std::num::NonZeroUsize;

use bytes::Bytes;
use eventsource_stream::Eventsource;
use futures::Stream;
use futures::StreamExt;
use serde_json::Value;

use crate::ChatStream;
use crate::ToolNames;
use crate::TranslationError;

#[derive(Debug, thiserror::Error)]
pub enum ProviderStreamError {
    #[error("provider SSE: {0}")]
    Transport(String),
    #[error("provider SSE JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Translation(#[from] TranslationError),
    #[error("provider stream ended without [DONE]")]
    Interrupted,
}

/// Adapts one provider HTTP body to Responses SSE frames. Parsing uses the
/// existing eventsource-stream library, including UTF-8 split across network
/// chunks, multiline data fields, comments and CRLF handling.
///
/// The raw byte budget applies before the SSE parser buffers an unfinished
/// event. Output is pulled incrementally, so a slow client backpressures the
/// provider body. Dropping the returned stream drops its source HTTP body.
pub fn translate_chat_sse<S, E>(
    source: S,
    response_id: String,
    byte_budget: NonZeroUsize,
    tools: ToolNames,
) -> impl Stream<Item = Result<Vec<u8>, ProviderStreamError>> + Send
where
    S: Stream<Item = Result<Bytes, E>> + Send,
    E: Display + Send,
{
    let mut remaining = byte_budget.get();
    let bounded = source.map(move |chunk| {
        let chunk = chunk.map_err(|error| io::Error::other(error.to_string()))?;
        remaining = remaining
            .checked_sub(chunk.len())
            .ok_or_else(|| io::Error::other("provider response exceeds byte budget"))?;
        Ok::<_, io::Error>(chunk)
    });
    async_stream::try_stream! {
        let events = bounded.eventsource();
        futures::pin_mut!(events);
        let mut converter = ChatStream::new(response_id, byte_budget);
        while let Some(event) = events.next().await {
            let event = event.map_err(|error| ProviderStreamError::Transport(error.to_string()))?;
            if event.data.is_empty() {
                continue;
            }
            let done = event.data == "[DONE]";
            let translated = if done {
                converter.finish()?
            } else {
                converter.push(&serde_json::from_str::<Value>(&event.data)?)?
            };
            for mut event in translated {
                tools.restore(&mut event);
                let kind = event["type"].as_str().ok_or_else(|| {
                    TranslationError::Invalid("Responses event type".to_owned())
                })?;
                let mut frame = format!("event: {kind}\ndata: ").into_bytes();
                serde_json::to_writer(&mut frame, &event)?;
                frame.extend_from_slice(b"\n\n");
                yield frame;
            }
            if done {
                return;
            }
        }
        Err(ProviderStreamError::Interrupted)?;
    }
}
