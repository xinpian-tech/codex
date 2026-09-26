use std::io;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::post;
use codex_infra_protocol::InferenceBinding;
use codex_infra_protocol::MessageId;
use futures::StreamExt;
use reqwest::Url;
use serde_json::Value;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::CustomTools;
use crate::ToolNames;
use crate::translate_chat_request;
use crate::translate_chat_sse;

/// One Agent's resolved account revision. The endpoint is the full provider
/// chat/completions URL; headers carry the selected account's authentication.
/// Updating credentials creates a new binding rather than mutating an attempt.
pub struct ChatFrontendConfig {
    pub binding: InferenceBinding,
    pub endpoint: Url,
    pub headers: HeaderMap,
    pub request_bytes: NonZeroUsize,
    pub response_bytes: NonZeroUsize,
    pub concurrent_requests: NonZeroUsize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

struct FrontendState {
    config: ChatFrontendConfig,
    client: reqwest::Client,
    permits: Arc<Semaphore>,
}

/// Dynamic loopback Responses endpoint for one logical provider client.
/// Graceful shutdown stops accepting requests and waits for active bodies.
pub struct ChatFrontend {
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl ChatFrontend {
    pub async fn start(config: ChatFrontendConfig) -> io::Result<Self> {
        if config.connect_timeout.is_zero() || config.request_timeout.is_zero() {
            return Err(io::Error::other("provider timeouts must be positive"));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .build()
            .map_err(io::Error::other)?;
        let limit = config.request_bytes.get();
        let permits = Arc::new(Semaphore::new(config.concurrent_requests.get()));
        let state = Arc::new(FrontendState {
            config,
            client,
            permits,
        });
        let router = Router::new()
            .route("/v1/responses", post(respond))
            .layer(DefaultBodyLimit::max(limit))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
        });
        Ok(Self {
            address,
            stop: Some(stop),
            task: Some(task),
        })
    }

    pub fn base_url(&self) -> String {
        format!("http://{}/v1", self.address)
    }

    pub async fn stop(mut self) -> io::Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.task
            .take()
            .ok_or_else(|| io::Error::other("provider frontend task missing"))?
            .await
            .map_err(io::Error::other)?
    }
}

impl Drop for ChatFrontend {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

async fn respond(State(state): State<Arc<FrontendState>>, bytes: Bytes) -> Response {
    let mut request = match serde_json::from_slice::<Value>(&bytes) {
        Ok(request) => request,
        Err(error) => return failure(StatusCode::BAD_REQUEST, error),
    };
    let custom = match CustomTools::normalize(&mut request) {
        Ok(custom) => custom,
        Err(error) => return failure(StatusCode::BAD_REQUEST, error),
    };
    let tools = match ToolNames::normalize(&mut request) {
        Ok(tools) => tools,
        Err(error) => return failure(StatusCode::BAD_REQUEST, error),
    };
    let request = match translate_chat_request(&request, &state.config.binding.model_id) {
        Ok(request) => request,
        Err(error) => return failure(StatusCode::BAD_REQUEST, error),
    };
    let permit = match Arc::clone(&state.permits).acquire_owned().await {
        Ok(permit) => permit,
        Err(error) => return failure(StatusCode::SERVICE_UNAVAILABLE, error),
    };
    let upstream = match state
        .client
        .post(state.config.endpoint.clone())
        .headers(state.config.headers.clone())
        .json(&request)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => return failure(StatusCode::BAD_GATEWAY, error),
    };
    let status = upstream.status();
    let mut headers = HeaderMap::new();
    for name in ["x-request-id", "request-id", "retry-after"] {
        if let Some(value) = upstream.headers().get(name) {
            headers.insert(name, value.clone());
        }
    }
    let body = if status.is_success() {
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("text/event-stream"),
        );
        headers.insert(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("no-cache"),
        );
        let stream = translate_chat_sse(
            upstream.bytes_stream(),
            format!("resp_{}", MessageId::new()),
            state.config.response_bytes,
            tools,
            custom,
        );
        Body::from_stream(async_stream::stream! {
            let _permit = permit;
            futures::pin_mut!(stream);
            while let Some(frame) = stream.next().await {
                yield frame;
            }
        })
    } else {
        if let Some(content_type) = upstream.headers().get(header::CONTENT_TYPE) {
            headers.insert(header::CONTENT_TYPE, content_type.clone());
        }
        let mut remaining = state.config.response_bytes.get();
        Body::from_stream(async_stream::stream! {
            let _permit = permit;
            let mut stream = upstream.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(io::Error::other).and_then(|chunk| {
                    remaining = remaining.checked_sub(chunk.len()).ok_or_else(|| io::Error::other("provider error body exceeds byte budget"))?;
                    Ok(chunk)
                });
                let failed = chunk.is_err();
                yield chunk;
                if failed {
                    return;
                }
            }
        })
    };
    (status, headers, body).into_response()
}

fn failure(status: StatusCode, error: impl std::fmt::Display) -> Response {
    (
        status,
        Json(json!({"error": {"message": error.to_string(), "type": "provider_adapter_error"}})),
    )
        .into_response()
}
