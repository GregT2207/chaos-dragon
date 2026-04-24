use std::{io::Result, net::UdpSocket};

pub struct Node {}

impl Node {
    pub fn start(&mut self) -> Result<()> {
        let socket = UdpSocket::bind("0.0.0.0:5000")?;
        let mut buf = [0; 1024];

        loop {
            let (amt, src) = socket.recv_from(&mut buf)?;
            let packet = &buf[..amt];
            println!("Received a packet from {}", src.ip())
        }
    }
}
