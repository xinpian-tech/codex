//! Machine-independent identities and task transitions for managed Agent processes.

mod contribution;
mod delivery;
mod identity;
mod message;
mod specification;
mod task;

pub use contribution::Contribution;
pub use contribution::ContributionError;
pub use contribution::ContributionProposal;
pub use contribution::ContributionStatus;
pub use delivery::DeliveryReceipt;
pub use delivery::DeliveryStage;
pub use delivery::FrameRoute;
pub use identity::AgentId;
pub use identity::AssignmentId;
pub use identity::CommitId;
pub use identity::ContributionId;
pub use identity::IdentityError;
pub use identity::MachineId;
pub use identity::MessageId;
pub use identity::RootSessionId;
pub use identity::TaskId;
pub use message::AgentMessage;
pub use message::MessageAddress;
pub use message::MessageKind;
pub use message::Presentation;
pub use specification::BuildIntent;
pub use specification::ConfigGeneration;
pub use specification::InferenceBinding;
pub use specification::TaskSpec;
pub use task::Assignment;
pub use task::AssignmentStatus;
pub use task::Dependency;
pub use task::TaskError;
pub use task::TaskRecord;
pub use task::TaskStatus;
