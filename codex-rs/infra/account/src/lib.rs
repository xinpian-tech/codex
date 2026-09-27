//! Account views and lifecycle integration around Codex's existing login library.

mod external;
mod publish;
mod remote;
mod service;
mod view;
mod wire;

pub use external::AccountCredentialSource;
pub use external::PublishedAccount;
pub use external::PublishedAccountAuth;
pub use publish::GitAccounts;
pub use remote::RemoteAccountSource;
pub use service::AccountService;
pub use service::AccountServiceConfig;
pub use service::AccountServiceState;
pub use view::CodexAccountView;
pub use wire::AccountAction;
pub use wire::AccountAuthority;
pub use wire::AccountRequest;
pub use wire::AccountResponse;
pub use wire::AccountResult;
