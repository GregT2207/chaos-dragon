use std::{
    io::{self, ErrorKind},
    net::IpAddr,
    str::{FromStr, from_utf8},
};

use crate::types::NodeId;

pub struct Transport {}

#[repr(usize)]
enum RawMessagePart {
    Direction,
    Kind,
    Payload,
}

pub struct Message<'a> {
    pub src_ip: IpAddr,
    pub direction: MessageDirection,
    pub kind: MessageKind,
    pub payload: Option<&'a str>,
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
    pub async fn request_identification(dest_ip: &IpAddr) -> NodeId {
        String::from("test")
    }

    pub fn parse_message<'a>(bytes: &'a [u8], src_ip: IpAddr) -> io::Result<Message<'a>> {
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
            payload: parts.get(RawMessagePart::Kind as usize).copied(),
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
