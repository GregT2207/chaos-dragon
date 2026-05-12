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
        let id = hostname::get()?.to_string_lossy().into_owned();
        let (transport_receiver, transport_sender) = new_transport(id.clone()).await?;

        Ok(Self {
            id: id.clone(),
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
                &message,
                &self.id,
                self.transport_sender.clone(),
                self.discovery.clone(),
            )
            .await;
        }

        Ok(())
    }

    pub async fn handle_message(
        message: &Message,
        node_id: &NodeId,
        transport_sender: TransportSender,
        discovery: Arc<Discovery>,
    ) {
        println!(
            "Received {:?} | {:?} | {:?} from {:?}",
            message.kind,
            message.direction,
            message
                .payload
                .clone()
                .unwrap_or("(no payload)".to_string()),
            message.src_node_id
        );

        match (&message.kind, &message.direction) {
            (MessageKind::Identity, MessageDirection::Request) => {
                if let Err(err) =
                    Self::respond_to_identification_request(&message, transport_sender).await
                {
                    eprintln!("Failed to respond to identification request: {}", err);
                };
            }
            (MessageKind::Identity, MessageDirection::Response) => {
                if let Err(err) = Self::handle_identification_response(&message, discovery).await {
                    eprintln!("Failed to handle identification response: {}", err);
                };
            }
        };
    }

    async fn respond_to_identification_request(
        message: &Message,
        transport_sender: TransportSender,
    ) -> Result<()> {
        match message.src_ip {
            Some(src_ip) => {
                transport_sender.respond_identification(src_ip).await?;

                Ok(())
            }
            None => {
                return Err(Error::other("No source IP address found"));
            }
        }
    }

    async fn handle_identification_response(
        message: &Message,
        discovery: Arc<Discovery>,
    ) -> Result<()> {
        if let Some(src_ip) = message.src_ip {
            discovery
                .record_sibling(message.src_node_id.clone(), src_ip)
                .await;
        }

        Ok(())
    }
}
