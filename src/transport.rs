use std::{
    env, fmt,
    io::{self, Error, ErrorKind},
    net::{IpAddr, SocketAddr},
    str::{FromStr, from_utf8},
    sync::Arc,
    time::Duration,
};

use log::{error, info, warn};
use time::OffsetDateTime;
use tokio::{net::UdpSocket, sync::Mutex, time::sleep};

use crate::{backoff::ExponentialBackoff, simulation::SimulatedState, types::NodeId};

type Buffer = [u8; 1024];

pub struct TransportReceiver {
    socket: Arc<UdpSocket>,
    buffer: Buffer,
    suspicious_inbound_message_gap: Duration,
    suspected_inbound_message_failure: bool,
    last_message_received: OffsetDateTime,
    simulated_state: Arc<SimulatedState>,
}

#[derive(Clone)]
pub struct TransportSender {
    socket: Arc<UdpSocket>,
    src_node_id: NodeId,
    internal_port: u16,
    simulated_state: Arc<SimulatedState>,
    backoff: Arc<Mutex<ExponentialBackoff>>,
}

#[repr(usize)]
enum RawMessagePart {
    SrcNodeId,
    Direction,
    Kind,
    Payload,
}

#[derive(Debug)]
pub struct Message {
    pub src_ip: Option<IpAddr>, // derived on message receipt, not added on message construction
    pub src_node_id: NodeId,
    pub direction: MessageDirection,
    pub kind: MessageKind,
    pub payload: Option<String>,
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}|{}|{}", self.src_node_id, self.direction, self.kind)?;
        if let Some(payload) = &self.payload {
            write!(f, "|{}", payload)?;
        }

        Ok(())
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum MessageDirection {
    Request,
    Response,
}

impl FromStr for MessageDirection {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "req" => Ok(MessageDirection::Request),
            "res" => Ok(MessageDirection::Response),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid message direction {s}"),
            )),
        }
    }
}

impl fmt::Display for MessageDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request => write!(f, "req"),
            Self::Response => write!(f, "res"),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum MessageKind {
    Identity,
    Gossip,
}

impl FromStr for MessageKind {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "id" => Ok(MessageKind::Identity),
            "goss" => Ok(MessageKind::Gossip),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid message kind {s}"),
            )),
        }
    }
}

impl fmt::Display for MessageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity => write!(f, "id"),
            Self::Gossip => write!(f, "goss"),
        }
    }
}

pub async fn new(
    src_node_id: NodeId,
    simulated_state: Arc<SimulatedState>,
) -> io::Result<(TransportReceiver, TransportSender)> {
    let internal_ip_addr = IpAddr::from([0, 0, 0, 0]);
    let internal_port = env::var("INTERNAL_PORT")
        .map_err(|e| Error::other(e))?
        .parse::<u16>()
        .map_err(|e| Error::other(e))?;
    let socket_addr = SocketAddr::new(internal_ip_addr, internal_port);

    let socket = Arc::new(UdpSocket::bind(socket_addr).await?);

    let suspicious_inbound_message_gap_ms = env::var("SUSPICIOUS_INBOUND_MESSAGE_GAP_MS")
        .map_err(|e| Error::other(e))?
        .parse::<u64>()
        .map_err(|e| Error::other(e))?;

    Ok((
        TransportReceiver {
            socket: Arc::clone(&socket),
            buffer: [0; 1024],
            suspicious_inbound_message_gap: Duration::from_millis(
                suspicious_inbound_message_gap_ms,
            ),
            suspected_inbound_message_failure: false,
            last_message_received: OffsetDateTime::now_utc(),
            simulated_state: Arc::clone(&simulated_state),
        },
        TransportSender {
            socket: Arc::clone(&socket),
            src_node_id,
            internal_port,
            simulated_state: Arc::clone(&simulated_state),
            backoff: Arc::new(Mutex::new(ExponentialBackoff::new())),
        },
    ))
}

impl TransportReceiver {
    pub async fn get_message(&mut self) -> Option<Message> {
        // After a suspiciously long gap in messages respond by throttling the loop slightly
        let time_since_last_message_received =
            OffsetDateTime::now_utc() - self.last_message_received;
        if time_since_last_message_received > self.suspicious_inbound_message_gap {
            self.suspected_inbound_message_failure = true;
            warn!(
                "No messages received in {} seconds, is there a network failure?",
                time_since_last_message_received.as_seconds_f32().floor()
            );
            sleep(Duration::from_secs(1)).await;
        }

        // Inbound network message failure would be silent so no error can be returned
        if !self.simulated_state.inbound_network_messages_available() {
            return None;
        }

        let received_message = self.receive_and_build_message().await;
        self.last_message_received = OffsetDateTime::now_utc();
        if self.suspected_inbound_message_failure == true {
            self.suspected_inbound_message_failure = false;
            info!("Received a message - inbound network availability confirmed")
        }

        match received_message {
            Ok(message) => Some(message),
            Err(err) => {
                error!("Failed to receive and build message: {err}");
                None
            }
        }
    }

    async fn receive_and_build_message(&mut self) -> io::Result<Message> {
        let (amt, src) = self.socket.recv_from(&mut self.buffer).await?;
        Self::build_message(&self.buffer[..amt], src.ip())
    }

    pub fn build_message(bytes: &[u8], src_ip: IpAddr) -> io::Result<Message> {
        let text = Self::parse_bytes_to_text(bytes)?;
        let parts = text.split('|').collect::<Vec<&str>>();

        Ok(Message {
            src_ip: Some(src_ip),
            src_node_id: (*(parts.get(RawMessagePart::SrcNodeId as usize).ok_or(
                io::Error::new(io::ErrorKind::InvalidData, "Missing source node ID"),
            )?))
            .to_string(),
            direction: (*(parts
                .get(RawMessagePart::Direction as usize)
                .ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Missing message direction",
                ))?))
            .parse()?,
            kind: (*(parts
                .get(RawMessagePart::Kind as usize)
                .ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Missing message kind",
                ))?))
            .parse()?,
            payload: parts
                .get(RawMessagePart::Payload as usize)
                .map(|s| s.to_string()),
        })
    }

    fn parse_bytes_to_text(bytes: &[u8]) -> io::Result<&str> {
        from_utf8(bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse incoming message as UTF-8 text: {e}"),
            )
        })
    }
}

impl TransportSender {
    pub async fn request_identification(&self, dest_ip: IpAddr) -> io::Result<()> {
        self.send_request_message(MessageKind::Identity, None, dest_ip)
            .await
    }

    pub async fn respond_identification(&self, dest_ip: IpAddr) -> io::Result<()> {
        self.send_response_message(MessageKind::Identity, None, dest_ip)
            .await
    }

    pub async fn request_gossip(&self, dest_ip: IpAddr) -> io::Result<()> {
        self.send_request_message(MessageKind::Gossip, None, dest_ip)
            .await
    }

    pub async fn respond_gossip(&self, dest_ip: IpAddr, payload: String) -> io::Result<()> {
        self.send_response_message(MessageKind::Gossip, Some(payload), dest_ip)
            .await
    }

    async fn send_request_message(
        &self,
        kind: MessageKind,
        payload: Option<String>,
        dest_ip: IpAddr,
    ) -> io::Result<()> {
        self.send_message(MessageDirection::Request, kind, payload, dest_ip)
            .await
    }

    async fn send_response_message(
        &self,
        kind: MessageKind,
        payload: Option<String>,
        dest_ip: IpAddr,
    ) -> io::Result<()> {
        self.send_message(MessageDirection::Response, kind, payload, dest_ip)
            .await
    }

    async fn send_message(
        &self,
        direction: MessageDirection,
        kind: MessageKind,
        payload: Option<String>,
        dest_ip: IpAddr,
    ) -> io::Result<()> {
        self.check_network_backoff().await?;

        let message = Message {
            src_ip: None,
            src_node_id: self.src_node_id.clone(),
            direction,
            kind,
            payload,
        };

        let destination_socket_addr = SocketAddr::new(dest_ip, self.internal_port);
        self.socket
            .send_to(message.to_string().as_bytes(), destination_socket_addr)
            .await?;

        Ok(())
    }

    async fn check_network_backoff(&self) -> io::Result<()> {
        let remaining_wait: Duration;
        {
            let mut backoff = self.backoff.lock().await;

            if self.simulated_state.outbound_network_messages_available() {
                backoff.reset();
                return Ok(());
            }

            remaining_wait = backoff.remaining_wait();
        }

        sleep(remaining_wait).await;
        return Err(Error::new(
            ErrorKind::NotFound,
            format!("Network unavailable"),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn it_parses_valid_utf8_bytes_to_text() {
        let text = "Hello, world!";
        let bytes = text.as_bytes();

        assert_eq!(
            TransportReceiver::parse_bytes_to_text(bytes).expect("Expected valid UTF-8 bytes"),
            text
        )
    }

    #[test]
    fn it_rejects_parsing_invalid_utf8_bytes() {
        let bytes: &[u8] = &[0xff, 0xfe, 0xfd];

        assert_eq!(
            TransportReceiver::parse_bytes_to_text(bytes)
                .expect_err("Expected invalid UTF-8 bytes")
                .kind(),
            ErrorKind::InvalidData
        )
    }

    #[test]
    fn it_rejects_invalid_message_direction() {
        let bytes = b"b62168f4fa9a|doesntexist|identity";
        let src_ip = "192.1.2.5".parse::<IpAddr>().unwrap();

        assert_eq!(
            TransportReceiver::build_message(bytes, src_ip)
                .expect_err("Expected invalid message bytes")
                .kind(),
            ErrorKind::InvalidData
        )
    }

    #[test]
    fn it_rejects_invalid_message_kind() {
        let bytes = b"b62168f4fa9a|req|doesntexist";
        let src_ip = "192.1.2.6".parse::<IpAddr>().unwrap();

        assert_eq!(
            TransportReceiver::build_message(bytes, src_ip)
                .expect_err("Expected invalid message bytes")
                .kind(),
            ErrorKind::InvalidData
        )
    }

    #[test]
    fn it_builds_identity_request_message() {
        let bytes = b"b62168f4fa9a|req|id";
        let src_ip = "192.1.2.3".parse::<IpAddr>().unwrap();

        let message = TransportReceiver::build_message(bytes, src_ip)
            .expect("Expected valid identity request message bytes");

        assert_eq!(message.src_node_id, "b62168f4fa9a");
        assert_eq!(message.direction, MessageDirection::Request);
        assert_eq!(message.kind, MessageKind::Identity);
        assert_eq!(message.payload, None);
    }

    #[test]
    fn it_builds_identity_response_message() {
        let bytes = b"b62168f4fa9a|res|id|7c736c19c8f0";
        let src_ip = "192.1.2.4".parse::<IpAddr>().unwrap();

        let message = TransportReceiver::build_message(bytes, src_ip)
            .expect("Expected valid identity response message bytes");

        assert_eq!(message.src_node_id, "b62168f4fa9a");
        assert_eq!(message.direction, MessageDirection::Response);
        assert_eq!(message.kind, MessageKind::Identity);
        assert_eq!(message.payload, Some("7c736c19c8f0".to_string()));
    }

    #[test]
    fn it_builds_gossip_request_message() {
        let bytes = b"b62168f4fa9a|req|goss";
        let src_ip = "192.1.2.5".parse::<IpAddr>().unwrap();

        let message = TransportReceiver::build_message(bytes, src_ip)
            .expect("Expected valid gossip request message bytes");

        assert_eq!(message.src_node_id, "b62168f4fa9a");
        assert_eq!(message.direction, MessageDirection::Request);
        assert_eq!(message.kind, MessageKind::Gossip);
        assert_eq!(message.payload, None);
    }

    #[test]
    fn it_builds_gossip_response_message() {
        let bytes = b"b62168f4fa9a|res|goss|7c736c19c8f0:192.2.1.1,7c736c19c8f1:192.2.1.2,7c736c19c8f2:192.2.1.3";
        let src_ip = "192.1.2.6".parse::<IpAddr>().unwrap();

        let message = TransportReceiver::build_message(bytes, src_ip)
            .expect("Expected valid gossip response message bytes");

        assert_eq!(message.src_node_id, "b62168f4fa9a");
        assert_eq!(message.direction, MessageDirection::Response);
        assert_eq!(message.kind, MessageKind::Gossip);
        assert_eq!(
            message.payload,
            Some(
                "7c736c19c8f0:192.2.1.1,7c736c19c8f1:192.2.1.2,7c736c19c8f2:192.2.1.3".to_string()
            )
        );
    }
}
