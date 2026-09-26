//! Provider wire adaptation, independent of Codex's agent and execution loop.
//! Account selection and endpoint realization belong to the launch binding.

mod request;

pub use request::TranslationError;
pub use request::translate_chat_request;
