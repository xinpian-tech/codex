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
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::post;
use codex_infra_protocol::InferenceBinding;
use futures::StreamExt;
use reqwest::Url;
use serde_json::Value;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::CustomTools;
use crate::ProviderAuditConfig;
use crate::ToolNames;
use crate::audit::AttemptAudit;
use crate::audit::WireLane;
use crate::translate_chat_request;
use crate::translate_chat_sse;

/// One Agent's resolved account revision. The endpoint is the full provider
/// chat/completions URL; headers carry the selected account's authentication.
/// Updating credentials creates a new binding rather than mutating an attempt.
pub struct ChatFrontendConfig {
    pub binding: InferenceBinding,
    pub audit: ProviderAuditConfig,
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
        let audit_directory = config.audit.directory.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&audit_directory)?;
            for entry in std::fs::read_dir(audit_directory)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    crate::recover_provider_attempt(&entry.path())?;
                }
            }
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(io::Error::other)??;
        let client = reqwest::Client::builder()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .build()
            .map_err(io::Error::other)?;
        let permits = Arc::new(Semaphore::new(config.concurrent_requests.get()));
        let state = Arc::new(FrontendState {
            config,
            client,
            permits,
        });
        let router = Router::new()
            .route("/v1/responses", post(respond))
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

async fn respond(State(state): State<Arc<FrontendState>>, request: Request) -> Response {
    let audit =
        match AttemptAudit::open(state.config.audit.clone(), state.config.binding.clone()).await {
            Ok(audit) => audit,
            Err(error) => return failure(StatusCode::INTERNAL_SERVER_ERROR, error),
        };
    let (parts, body) = request.into_parts();
    if let Err(error) = audit.event(json!({"event": "client_request_headers", "method": parts.method.as_str(),
        "uri": parts.uri.to_string(),
        "headers": parts.headers.iter().map(|(name, value)| (name.as_str(), value.as_bytes())).collect::<Vec<_>>(),
    })).await {
        return failure(StatusCode::INTERNAL_SERVER_ERROR, error);
    }
    let received = async {
        let stream = audit.clone().capture(body.into_data_stream(), WireLane::ClientRequest);
        futures::pin_mut!(stream);
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.len() > state.config.request_bytes.get().saturating_sub(bytes.len()) {
                audit.event(json!({"event": "client_request_limit", "limit": state.config.request_bytes.get()})).await?;
                return Err(io::Error::new(io::ErrorKind::FileTooLarge, "provider request exceeds byte budget"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(bytes))
    }.await;
    let response = match received {
        Ok(bytes) => execute(state, audit.clone(), bytes).await,
        Err(error) => {
            let status = if error.kind() == io::ErrorKind::FileTooLarge {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            };
            failure(status, error)
        }
    };
    let (parts, body) = response.into_parts();
    if let Err(error) = audit.event(json!({"event": "client_response_headers", "status": parts.status.as_u16(),
        "headers": parts.headers.iter().map(|(name, value)| (name.as_str(), value.as_bytes())).collect::<Vec<_>>(),
    })).await {
        return failure(StatusCode::INTERNAL_SERVER_ERROR, error);
    }
    Response::from_parts(
        parts,
        Body::from_stream(audit.capture(body.into_data_stream(), WireLane::ClientResponse)),
    )
}

async fn execute(state: Arc<FrontendState>, audit: AttemptAudit, bytes: Bytes) -> Response {
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
    let body = match serde_json::to_vec(&request) {
        Ok(body) => Bytes::from(body),
        Err(error) => return failure(StatusCode::INTERNAL_SERVER_ERROR, error),
    };
    if let Err(error) = audit.record(WireLane::ProviderRequest, body.clone()).await {
        return failure(StatusCode::INTERNAL_SERVER_ERROR, error);
    }
    let request = match state
        .client
        .post(state.config.endpoint.clone())
        .headers(state.config.headers.clone())
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .build()
    {
        Ok(request) => request,
        Err(error) => return failure(StatusCode::INTERNAL_SERVER_ERROR, error),
    };
    if let Err(error) = audit.event(json!({"event": "send_requested", "url": request.url().as_str(),
        "headers": request.headers().iter().map(|(name, value)| (name.as_str(), value.as_bytes())).collect::<Vec<_>>(),
    })).await {
        return failure(StatusCode::INTERNAL_SERVER_ERROR, error);
    }
    let upstream = match state.client.execute(request).await {
        Ok(response) => response,
        Err(error) => {
            if let Err(recording) = audit
                .event(json!({"event": "send_failed", "error": error.to_string()}))
                .await
            {
                return failure(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("{error}; audit: {recording}"),
                );
            }
            return failure(StatusCode::BAD_GATEWAY, error);
        }
    };
    let status = upstream.status();
    if let Err(error) = audit.event(json!({"event": "response_headers", "status": status.as_u16(),
        "headers": upstream.headers().iter().map(|(name, value)| (name.as_str(), value.as_bytes())).collect::<Vec<_>>(),
    })).await {
        return failure(StatusCode::INTERNAL_SERVER_ERROR, error);
    }
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
            audit
                .clone()
                .capture(upstream.bytes_stream(), WireLane::ProviderResponse),
            format!("resp_{}", audit.id),
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
            let stream = audit.clone().capture(upstream.bytes_stream(), WireLane::ProviderResponse);
            futures::pin_mut!(stream);
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
