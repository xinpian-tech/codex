use std::sync::Arc;

use codex_core::config::Config;
use codex_extension_api::ExtensionRegistry;
use codex_thread_store::ThreadStore;

/// Selects who owns memory generation; memory reading is configured separately.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum MemoryGeneration {
    BuiltIn,
    ManagedTasks,
}

/// Process-scoped services supplied by an embedding host.
///
/// Methods receive the normal app-server services once during startup. Hosts
/// can retain or decorate them, or supply replacements. The returned services
/// are shared by newly started, resumed, and forked threads for this process.
/// Implementations should do fallible preparation before starting the host.
pub trait HostServices: Send + Sync {
    /// Lets independent Agent hosts route memory generation through their own
    /// task lifecycle while retaining the existing memory read extensions.
    fn memory_generation(&self) -> MemoryGeneration {
        MemoryGeneration::BuiltIn
    }

    /// Supplies one account resolver for both bootstrap and serving auth.
    /// The host owns credential refresh and publication; every embedded manager
    /// installs this provider before being exposed to request consumers.
    fn external_auth(&self) -> Option<Arc<dyn codex_login::ExternalAuth>> {
        None
    }

    /// Selects persistence without changing app-server's separate queue store.
    fn thread_store(&self, default: Arc<dyn ThreadStore>) -> Arc<dyn ThreadStore> {
        default
    }

    /// Selects typed contributions after app-server installs its defaults.
    /// Use `to_builder` to preserve defaults while adding host contributions.
    fn extensions(
        &self,
        default: Arc<ExtensionRegistry<Config>>,
    ) -> Arc<ExtensionRegistry<Config>> {
        default
    }
}

pub(crate) struct DefaultHostServices;

impl HostServices for DefaultHostServices {}
