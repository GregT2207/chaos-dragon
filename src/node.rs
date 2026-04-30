use std::{
    env,
    io::{Error, Result},
    sync::Arc,
    time::Duration,
};

use tokio::{sync::mpsc, task, time::interval};

use crate::{
    discovery::Discovery,
    transport::{
        Message, MessageDirection, MessageKind, TransportReceiver, TransportSender,
        new as new_transport,
    },
    types::NodeId,
};

pub struct Node {
    id: NodeId,
    transport_receiver: TransportReceiver,
    transport_sender: TransportSender,
    discovery: Arc<Discovery>,
}

impl Node {
    pub async fn new() -> Result<Self> {
        let (transport_receiver, transport_sender) = new_transport().await?;

        Ok(Self {
            id: hostname::get()?.to_string_lossy().into_owned(),
            transport_receiver,
            transport_sender,
            discovery: Arc::new(Discovery::new()?),
        })
    }

    pub async fn start(self) -> Result<()> {
        println!("Starting up node {}", self.id);

        let transport_sender = self.transport_sender.clone();
        let discovery = Arc::clone(&self.discovery);
        let discover_siblings_interval_ms = env::var("DISCOVER_SIBLINGS_INTERVAL_MS")
            .map_err(|e| Error::other(e))?
            .parse::<u64>()
            .map_err(|e| Error::other(e))?;
        let mut interval = interval(Duration::from_millis(discover_siblings_interval_ms));
        task::spawn(async move {
            loop {
                interval.tick().await;
                discovery.discover_siblings(transport_sender.clone()).await;
            }
        });

        let (tx, mut rx) = mpsc::channel::<Message>(32);
        let mut transport_receiver = self.transport_receiver;
        task::spawn(async move {
            loop {
                if let Some(message) = transport_receiver.get_message().await {
                    if let Err(err) = tx.send(message).await {
                        eprintln!("Error sending transport message into channel: {}", err)
                    }
                };
            }
        });

        while let Some(message) = rx.recv().await {
            Self::handle_message(
                self.transport_sender.clone(),
                self.discovery.clone(),
                &message,
            )
            .await;
        }

        Ok(())
    }

    pub async fn handle_message(
        transport_sender: TransportSender,
        discovery: Arc<Discovery>,
        message: &Message,
    ) {
        println!(
            "Received {:?} | {:?} | {:?} from {:?}",
            message.kind,
            message.direction,
            message
                .payload
                .clone()
                .unwrap_or("(no payload)".to_string()),
            message.src_ip
        );

        match (&message.kind, &message.direction) {
            (MessageKind::Identity, MessageDirection::Request) => (),
            (MessageKind::Identity, MessageDirection::Response) => (),
        };
    }
}
