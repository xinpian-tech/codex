//! Machine and Agent runtime services, independent of the Codex inference engine.

mod collector;
mod mailbox;
mod recorded_writer;

pub use collector::CollectorCompletion;
pub use collector::ControlCollector;
pub use mailbox::HostMailbox;
