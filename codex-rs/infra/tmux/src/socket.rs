use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;

use codex_infra_protocol::DeliveryReceipt;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;

use crate::FrameChunk;

const MAX_PACKET_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum GatewayPacket {
    Chunk(FrameChunk),
    Receipt(DeliveryReceipt),
}

/// One listener per machine/Root Session. The OS assigns the port on every bind;
/// callers publish `endpoint()` together with their current gateway instance.
pub struct GatewayListener {
    listener: TcpListener,
}

impl GatewayListener {
    pub async fn bind(address: IpAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(SocketAddr::new(address, /*port*/ 0)).await?;
        Ok(Self { listener })
    }

    pub fn endpoint(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn accept(&self) -> io::Result<(GatewayConnection, SocketAddr)> {
        let (stream, address) = self.listener.accept().await?;
        Ok((GatewayConnection { stream }, address))
    }
}

/// Plain TCP transports tmux frames and receipt metadata. Semantic chunks sent
/// here originate in the sender pane; received chunks must be injected into the
/// recipient pane before the recipient host can present their contents.
pub struct GatewayConnection {
    stream: TcpStream,
}

impl GatewayConnection {
    pub async fn connect(endpoint: SocketAddr) -> io::Result<Self> {
        let stream = TcpStream::connect(endpoint).await?;
        Ok(Self { stream })
    }

    pub fn split(self) -> (GatewaySender, GatewayReceiver) {
        let (reader, writer) = self.stream.into_split();
        (GatewaySender { writer }, GatewayReceiver { reader })
    }
}

pub struct GatewaySender {
    writer: OwnedWriteHalf,
}

impl GatewaySender {
    pub async fn send(&mut self, packet: &GatewayPacket) -> io::Result<()> {
        let bytes = serde_json::to_vec(packet).map_err(io::Error::other)?;
        if bytes.len() > MAX_PACKET_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "gateway packet exceeds frame size",
            ));
        }
        self.writer
            .write_u32(u32::try_from(bytes.len()).map_err(io::Error::other)?)
            .await?;
        self.writer.write_all(&bytes).await?;
        self.writer.flush().await
    }
}

pub struct GatewayReceiver {
    reader: OwnedReadHalf,
}

impl GatewayReceiver {
    /// A canceled partial read retires this connection; replay on a new connection
    /// uses persisted message/chunk IDs and the recipient's durable inbox.
    pub async fn receive(&mut self) -> io::Result<GatewayPacket> {
        let length = self.reader.read_u32().await? as usize;
        if length > MAX_PACKET_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gateway packet exceeds frame size",
            ));
        }
        let mut bytes = vec![0; length];
        self.reader.read_exact(&mut bytes).await?;
        let packet: GatewayPacket = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        match &packet {
            GatewayPacket::Chunk(chunk) => {
                chunk.payload()?;
            }
            GatewayPacket::Receipt(_) => {}
        }
        Ok(packet)
    }
}
