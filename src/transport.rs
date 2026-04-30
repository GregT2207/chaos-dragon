use std::{
    io::{self, ErrorKind},
    net::IpAddr,
    str::{FromStr, from_utf8},
};

use tokio::net::UdpSocket;

use crate::types::NodeId;

const EXPOSED_ADDRESS: &str = "0.0.0.0:3000";

pub struct Transport {
    socket: UdpSocket,
    buffer: [u8; 1024],
}

#[repr(usize)]
enum RawMessagePart {
    Direction,
    Kind,
    Payload,
}

#[derive(Debug)]
pub struct Message {
    pub src_ip: IpAddr,
    pub direction: MessageDirection,
    pub kind: MessageKind,
    pub payload: Option<String>,
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

#[derive(PartialEq, Eq, Debug)]
pub enum MessageKind {
    Identity,
}

impl FromStr for MessageKind {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "id" => Ok(MessageKind::Identity),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid message kind {s}"),
            )),
        }
    }
}

impl Transport {
    pub async fn new() -> io::Result<Self> {
        Ok(Self {
            socket: UdpSocket::bind(EXPOSED_ADDRESS).await?,
            buffer: [0; 1024],
        })
    }

    pub async fn get_message(&mut self) -> Option<Message> {
        match self.receive_and_build_message().await {
            Ok(message) => Some(message),
            Err(err) => {
                eprintln!("Failed to receive and parse message: {err}");
                None
            }
        }
    }

    async fn receive_and_build_message(&mut self) -> io::Result<Message> {
        let (amt, src) = self.socket.recv_from(&mut self.buffer).await?;
        Self::build_message(&self.buffer[..amt], src.ip())
    }

    pub async fn request_identification(dest_ip: &IpAddr) -> NodeId {
        String::from("test")
    }

    pub fn build_message(bytes: &[u8], src_ip: IpAddr) -> io::Result<Message> {
        let text = Self::bytes_to_text(bytes)?;
        let parts = text.split('|').collect::<Vec<&str>>();

        Ok(Message {
            src_ip,
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

    fn bytes_to_text(bytes: &[u8]) -> io::Result<&str> {
        from_utf8(bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse incoming message as UTF-8 text: {e}"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_valid_utf8_bytes_to_text() {
        let text = "Hello, world!";
        let bytes = text.as_bytes();

        assert_eq!(
            Transport::bytes_to_text(bytes).expect("Expected valid UTF-8 bytes"),
            text
        )
    }

    #[test]
    fn it_rejects_invalid_utf8_bytes() {
        let bytes: &[u8] = &[0xff, 0xfe, 0xfd];

        assert_eq!(
            Transport::bytes_to_text(bytes)
                .expect_err("Expected invalid UTF-8 bytes")
                .kind(),
            ErrorKind::InvalidData
        )
    }

    #[test]
    fn it_builds_identity_request_message() {
        let bytes = b"req|id";
        let src_ip = "192.1.2.3".parse::<IpAddr>().unwrap();

        let message = Transport::build_message(bytes, src_ip)
            .expect("Expected valid identity request message bytes");

        assert_eq!(message.src_ip, src_ip);
        assert_eq!(message.direction, MessageDirection::Request);
        assert_eq!(message.kind, MessageKind::Identity);
        assert_eq!(message.payload, None);
    }

    #[test]
    fn it_builds_identity_response_message() {
        let bytes = b"res|id|7c736c19c8f0";
        let src_ip = "192.4.5.6".parse::<IpAddr>().unwrap();

        let message = Transport::build_message(bytes, src_ip)
            .expect("Expected valid identity response message bytes");

        assert_eq!(message.src_ip, src_ip);
        assert_eq!(message.direction, MessageDirection::Response);
        assert_eq!(message.kind, MessageKind::Identity);
        assert_eq!(message.payload, Some("7c736c19c8f0".to_string()));
    }

    #[test]
    fn it_rejects_invalid_message_direction() {
        let bytes = b"doesntexist|identity";
        let src_ip = "192.7.8.9".parse::<IpAddr>().unwrap();

        assert_eq!(
            Transport::build_message(bytes, src_ip)
                .expect_err("Expected invalid message bytes")
                .kind(),
            ErrorKind::InvalidData
        )
    }

    #[test]
    fn it_rejects_invalid_message_kind() {
        let bytes = b"req|doesntexist";
        let src_ip = "192.7.8.9".parse::<IpAddr>().unwrap();

        assert_eq!(
            Transport::build_message(bytes, src_ip)
                .expect_err("Expected invalid message bytes")
                .kind(),
            ErrorKind::InvalidData
        )
    }
}
