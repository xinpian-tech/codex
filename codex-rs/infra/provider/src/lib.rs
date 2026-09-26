//! Provider wire adaptation, independent of Codex's agent and execution loop.
//! Account selection and endpoint realization belong to the launch binding.

mod audit;
mod custom;
mod frontend;
mod request;
mod sse;
mod stream;
mod tools;

// Responses names its opaque continuation field encrypted_content. This
// adapter's versioned payload is plaintext provider reasoning, not ciphertext.
pub(crate) const CHAT_REASONING_PREFIX: &str = "codex-infra-chat-reasoning-v1:";

pub use audit::ProviderAuditConfig;
pub use custom::CustomTools;
pub use frontend::ChatFrontend;
pub use frontend::ChatFrontendConfig;
pub use request::TranslationError;
pub use request::translate_chat_request;
pub use sse::ProviderStreamError;
pub use sse::translate_chat_sse;
pub use stream::ChatStream;
pub use tools::ToolNames;
