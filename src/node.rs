use std::{
    env,
    io::{Error, Result},
    sync::Arc,
    time::Duration,
};

use tokio::{sync::mpsc, task, time::interval};

use crate::{
    discovery::Discovery,
    transport::{Message, Transport},
    types::NodeId,
};

pub struct Node {
    id: NodeId,
    transport: Transport,
    discovery: Arc<Discovery>,
}

impl Node {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            id: hostname::get()?.to_string_lossy().into_owned(),
            transport: Transport::new().await?,
            discovery: Arc::new(Discovery::new()?),
        })
    }

    pub async fn start(self) -> Result<()> {
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
                discovery.discover_siblings().await;
            }
        });

        let mut transport = self.transport;
        let (tx, mut rx) = mpsc::channel::<Message>(32);
        task::spawn(async move {
            loop {
                if let Some(message) = transport.get_message().await {
                    if let Err(err) = tx.send(message).await {
                        eprintln!("Error sending transport message into channel: {}", err)
                    }
                };
            }
        });

        while let Some(message) = rx.recv().await {
            Self::handle_message(message).await;
        }

        Ok(())
    }

    pub async fn handle_message(message: Message) {
        if let Some(payload) = message.payload {
            println!("Received {} from {}", payload, message.src_ip)
        }
    }
}
