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

pub struct Message {
    pub src_ip: IpAddr,
    pub direction: MessageDirection,
    pub kind: MessageKind,
    pub payload: Option<String>,
}

pub enum MessageDirection {
    Request,
    Response,
}

impl FromStr for MessageDirection {
    type Err = ErrorKind;

    fn from_str(s: &str) -> Result<Self, ErrorKind> {
        match s {
            "req" => Ok(MessageDirection::Request),
            "res" => Ok(MessageDirection::Response),
            _ => Err(ErrorKind::InvalidData),
        }
    }
}

pub enum MessageKind {
    Identity,
}

impl FromStr for MessageKind {
    type Err = ErrorKind;

    fn from_str(s: &str) -> Result<Self, ErrorKind> {
        match s {
            "id" => Ok(MessageKind::Identity),
            _ => Err(ErrorKind::InvalidData),
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
        match self.receive_and_parse_message().await {
            Ok(message) => Some(message),
            Err(err) => {
                eprintln!("Failed to receive and parse message: {}", err);
                None
            }
        }
    }

    async fn receive_and_parse_message(&mut self) -> io::Result<Message> {
        let (amt, src) = self.socket.recv_from(&mut self.buffer).await?;
        Self::parse_message(&self.buffer[..amt], src.ip())
    }

    pub async fn request_identification(dest_ip: &IpAddr) -> NodeId {
        String::from("test")
    }

    pub fn parse_message(bytes: &[u8], src_ip: IpAddr) -> io::Result<Message> {
        let text = Self::bytes_to_text(bytes)?;
        let parts = text.split('|').collect::<Vec<&str>>();

        Ok(Message {
            src_ip,
            direction: (*(parts
                .get(RawMessagePart::Direction as usize)
                .ok_or(io::ErrorKind::InvalidData)?))
            .parse()?,
            kind: (*(parts
                .get(RawMessagePart::Kind as usize)
                .ok_or(ErrorKind::InvalidData)?))
            .parse()?,
            payload: parts
                .get(RawMessagePart::Kind as usize)
                .map(|s| s.to_string()),
        })
    }

    fn bytes_to_text(bytes: &[u8]) -> io::Result<&str> {
        from_utf8(bytes).map_err(|e| {
            io::Error::other(format!(
                "Failed to parse incoming message as UTF-8 text: {}",
                e
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_utf8_bytes_parse_to_text() {
        let bytes: &[u8] = &[
            0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x21,
        ];

        assert_eq!(
            Transport::bytes_to_text(bytes).expect("Expected valid UTF-8 bytes"),
            "Hello, world!"
        )
    }

    #[test]
    fn invalid_utf8_bytes_fail() {
        let bytes: &[u8] = &[0xff, 0xfe, 0xfd];

        assert_eq!(
            Transport::bytes_to_text(bytes)
                .expect_err("Expected invalid UTF-8 bytes")
                .kind(),
            ErrorKind::Other
        )
    }
}
