//! Provider wire adaptation, independent of Codex's agent and execution loop.
//! Account selection and endpoint realization belong to the launch binding.

mod request;
mod stream;

// Responses names its opaque continuation field encrypted_content. This
// adapter's versioned payload is plaintext provider reasoning, not ciphertext.
pub(crate) const CHAT_REASONING_PREFIX: &str = "codex-infra-chat-reasoning-v1:";

pub use request::TranslationError;
pub use request::translate_chat_request;
pub use stream::ChatStream;
