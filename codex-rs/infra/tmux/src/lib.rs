//! Framing and tmux control-mode decoding for managed Agent pane traffic.

mod assembly;
mod client;
mod control;
mod control_decoder;
mod frame;
mod socket;
#[cfg(unix)]
mod terminal;

pub use assembly::MessageAssembler;
pub use client::AgentLaunch;
pub use client::PanePlacement;
pub use client::PaneProcess;
pub use client::PaneProcessState;
pub use client::TmuxClient;
pub use codex_infra_protocol::DeliveryReceipt;
pub use codex_infra_protocol::DeliveryStage;
pub use codex_infra_protocol::FrameRoute;
pub use control::PaneOutput;
pub use control::parse_pane_output;
pub use control_decoder::ControlDecoder;
pub use control_decoder::ControlRecord;
pub use frame::FrameChunk;
pub use frame::FrameDecoder;
pub use frame::HostReady;
pub use frame::TransportFrame;
pub use frame::write_message;
pub use socket::GatewayConnection;
pub use socket::GatewayListener;
pub use socket::GatewayReceiver;
pub use socket::GatewaySender;
#[cfg(unix)]
pub use terminal::AgentTerminal;
