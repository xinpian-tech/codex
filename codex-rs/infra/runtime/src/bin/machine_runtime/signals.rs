use std::io;

use serde::Serialize;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StopReason {
    Requested,
    StdinClosed,
    Interrupt,
    #[cfg(unix)]
    Terminate,
    ControlError,
}

pub(super) struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(windows)]
    interrupt: tokio::signal::windows::CtrlC,
}

impl ShutdownSignals {
    /// Register before opening machine resources. Signals arriving during
    /// startup remain pending until the control loop can perform owned teardown.
    pub(super) fn open() -> io::Result<Self> {
        Ok(Self {
            #[cfg(unix)]
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
            #[cfg(windows)]
            interrupt: tokio::signal::windows::ctrl_c()?,
        })
    }

    pub(super) async fn receive(&mut self) -> io::Result<StopReason> {
        #[cfg(unix)]
        {
            tokio::select! {
                event = self.interrupt.recv() => event.map(|()| StopReason::Interrupt)
                    .ok_or_else(|| io::Error::other("interrupt signal stream closed")),
                event = self.terminate.recv() => event.map(|()| StopReason::Terminate)
                    .ok_or_else(|| io::Error::other("terminate signal stream closed")),
            }
        }
        #[cfg(windows)]
        {
            self.interrupt
                .recv()
                .await
                .map(|()| StopReason::Interrupt)
                .ok_or_else(|| io::Error::other("interrupt signal stream closed"))
        }
        #[cfg(not(any(unix, windows)))]
        std::future::pending().await
    }
}
