use std::sync::Arc;

use codex_core::config::Config;
use codex_extension_api::ExtensionRegistry;
use codex_thread_store::ThreadStore;

/// Process-scoped services supplied by an embedding host.
///
/// Methods receive the normal app-server services once during startup. Hosts
/// can retain or decorate them, or supply replacements. The returned services
/// are shared by newly started, resumed, and forked threads for this process.
/// Implementations should do fallible preparation before starting the host.
pub trait HostServices: Send + Sync {
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
