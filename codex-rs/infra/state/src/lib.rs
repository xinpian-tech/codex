//! Durable machine-local records backing Git Session publication.

mod checkpoint;
mod journal;
mod tasks;
mod workspace;

pub use checkpoint::CheckpointAttempt;
pub use checkpoint::CheckpointCoordinator;
pub use checkpoint::CheckpointKind;
pub use checkpoint::CheckpointPhase;
pub use journal::Journal;
pub use journal::JournalRecord;
pub use tasks::TaskStore;
pub use tasks::TaskStoreError;
pub use workspace::Checkpoint;
pub use workspace::GitWorkspace;
pub use workspace::WorkspaceBinding;
