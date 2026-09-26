//! Framing and tmux control-mode decoding for managed Agent pane traffic.

mod client;
mod control;
mod frame;

pub use client::AgentLaunch;
pub use client::PanePlacement;
pub use client::TmuxClient;
pub use control::PaneOutput;
pub use control::parse_pane_output;
pub use frame::FrameChunk;
pub use frame::FrameDecoder;
pub use frame::FrameRoute;
pub use frame::write_message;
