//! Managed Agent contributions and adapters for the existing Codex host.

mod binding;
mod checkpoints;
mod context;
mod host;
mod process_activity;
mod process_audit;
mod store;
mod store_audit;
mod tool_audit;

pub use checkpoints::WorkspaceCheckpoints;
pub use context::AgentContext;
pub use host::ManagedHost;
pub use host::ManagedHostServices;
pub use process_activity::ProcessActivity;
pub use process_activity::ProcessSettlement;
pub use process_activity::ProcessSettlementOutcome;
pub use process_audit::ProcessAudit;
pub use process_audit::ProcessAuditEvent;
pub use store::AuditedThreadStore;
pub use store_audit::StoreAudit;
pub use store_audit::StoreAuditEvent;
pub use store_audit::StoreAuditIdentity;
pub use tool_audit::RecordedToolPayload;
pub use tool_audit::ToolAudit;
pub use tool_audit::ToolAuditEvent;
pub use tool_audit::ToolOperation;
pub use tool_audit::ToolOrigin;
pub use tool_audit::ToolOutcome;
