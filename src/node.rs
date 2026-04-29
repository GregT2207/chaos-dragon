use std::{
    env,
    io::{Error, Result},
    sync::Arc,
    time::Duration,
};

use tokio::{net::UdpSocket, task, time::interval};

use crate::{
    discovery::Discovery,
    transport::{Message, Transport},
    types::NodeId,
};

pub struct Node {
    id: NodeId,
    discovery: Arc<Discovery>,
}

const EXPOSED_ADDRESS: &str = "0.0.0.0:3000";

impl Node {
    pub fn new() -> Result<Self> {
        Ok(Self {
            id: hostname::get()?.to_string_lossy().into_owned(),
            discovery: Arc::new(Discovery::new()?),
        })
    }

    pub async fn start(&self) -> Result<()> {
        println!("Starting up node {}", self.id);

        let discovery = Arc::clone(&self.discovery);
        let discover_siblings_interval_ms = env::var("DISCOVER_SIBLINGS_INTERVAL_MS")
            .map_err(|e| Error::other(e))?
            .parse::<u64>()
            .map_err(|e| Error::other(e))?;
        let mut interval = interval(Duration::from_millis(discover_siblings_interval_ms));
        task::spawn(async move {
            loop {
                interval.tick().await;
                Arc::clone(&discovery).discover_siblings().await;
            }
        });

        let socket = UdpSocket::bind(EXPOSED_ADDRESS).await?;
        let mut buf = [0; 1024];
        loop {
            let (amt, src) = socket.recv_from(&mut buf).await?;
            let message = Transport::parse_message(&buf[..amt], src.ip());
            match message {
                Ok(message) => self.handle_message(message).await,
                Err(err) => eprintln!("Failed to parse message: {}", err),
            }
        }
    }

    async fn handle_message(&self, message: Message<'_>) {
        if let Some(payload) = message.payload {
            println!("Received {} from {}", payload, message.src_ip)
        }
    }
}
