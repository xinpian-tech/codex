//! Nix resolution binds declared work to immutable flake sources and derivations.

mod generation;
mod resolver;

pub use resolver::BuildOutput;
pub use resolver::BuildPlan;
pub use resolver::LockedFlake;
pub use resolver::NixTaskResolver;
pub use resolver::ResolvedTask;
