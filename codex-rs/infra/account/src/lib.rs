//! Account views and lifecycle integration around Codex's existing login library.

mod external;
mod view;

pub use external::AccountCredentialSource;
pub use external::PublishedAccount;
pub use external::PublishedAccountAuth;
pub use view::CodexAccountView;
