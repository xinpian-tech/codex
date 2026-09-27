//! Request-scoped interception of model response streams.
pub use codex_api::ApiError as ModelResponseError;
pub use codex_api::ResponseEvent;
use futures::stream::BoxStream;
use std::collections::HashMap;

/// Final transport input after wire preparation and size bounding. WebSocket
/// continuation can contain only a delta; this is not the entire conversation.
/// Observation precedes transport submission and does not prove provider receipt.
#[derive(Clone, Copy)]
pub struct ModelRequestObservation<'a> {
    pub kind: ModelRequestKind,
    pub thread_id: &'a str,
    pub turn_id: Option<&'a str>,
    pub model: &'a str,
    pub input: &'a [codex_protocol::models::ResponseItem],
    pub previous_response_id: Option<&'a str>,
}

/// Model response events owned by an interceptor or its downstream consumer.
pub type ModelResponseStream = BoxStream<'static, Result<ResponseEvent, ModelResponseError>>;

/// Whether a request generates output or only prepares a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelRequestKind {
    /// Generate model output.
    Generation,
    /// Prepare the session without generating output.
    Warmup,
}

/// Request identity and additional metadata supplied before sending to the provider.
pub struct ModelRequestInput<'a> {
    /// Whether this request generates output or warms up the session.
    pub kind: ModelRequestKind,
    /// Thread making the request.
    pub thread_id: &'a str,
    /// Additional provider metadata for request tracking; existing and reserved keys cannot be replaced.
    pub client_metadata: &'a mut Option<HashMap<String, String>>,
    /// Requested model identifier.
    pub model: &'a str,
}

/// Creates request-scoped interceptors and observes prepared transport inputs.
/// Return None from request for streams this host does not intercept; observe
/// defaults to no work for contributors that do not need request evidence.
pub trait ModelRequestContributor: Send + Sync + std::fmt::Debug {
    fn request(&self, input: ModelRequestInput<'_>) -> Option<Box<dyn ModelResponseInterceptor>>;

    /// Completes host-owned observation before sending this transport attempt.
    /// Implementations must leave input unchanged and move blocking I/O off the
    /// executor. Returning an error prevents this attempt from being submitted.
    /// An optional interceptor observes the response for this exact attempt,
    /// before the interceptors returned by request transform its events.
    fn observe<'a>(
        &'a self,
        _input: ModelRequestObservation<'a>,
    ) -> crate::ExtensionFuture<'a, std::io::Result<Option<Box<dyn ModelResponseInterceptor>>>>
    {
        Box::pin(async { Ok(None) })
    }
}

/// Owns request state until consumed by the response stream or dropped on failure.
/// Interceptors compose in registration order; dropping their stream must release unfinished work.
/// Trusted host implementations must bound any model-visible content they synthesize.
pub trait ModelResponseInterceptor: Send + Sync {
    /// Wrap the response stream, optionally buffering, inspecting, or transforming events.
    /// The returned stream must own request state and propagate cancellation by dropping upstream.
    fn intercept(self: Box<Self>, stream: ModelResponseStream) -> ModelResponseStream;
}
