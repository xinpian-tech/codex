//! Durable machine-local records backing Git Session publication.

mod checkpoint;
mod inbox;
mod journal;
mod session;
mod tasks;
mod workspace;

pub use checkpoint::CheckpointAttempt;
pub use checkpoint::CheckpointCoordinator;
pub use checkpoint::CheckpointKind;
pub use checkpoint::CheckpointPhase;
pub use inbox::DurableInbox;
pub use inbox::InboxEntry;
pub use inbox::PresentedInput;
pub use journal::Journal;
pub use journal::JournalRecord;
pub use session::ArchiveReceipt;
pub use session::SessionShard;
pub use tasks::TaskStore;
pub use tasks::TaskStoreError;
pub use workspace::Checkpoint;
pub use workspace::GitWorkspace;
pub use workspace::WorkspaceBinding;
