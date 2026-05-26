use std::{
    env,
    io::{Error, Result},
    net::IpAddr,
    sync::Arc,
    time::Duration,
};

use futures::future::join_all;
use log::{debug, error, info};
use time::OffsetDateTime;
use tokio::{sync::mpsc, task, time::interval};

use crate::{
    discovery::{Discovery, Sibling, SiblingsMap},
    simulation::SimulatedState,
    transport::{
        Message, MessageDirection, MessageKind, TransportReceiver, TransportSender,
        new as new_transport,
    },
    types::NodeId,
};

pub struct Node {
    id: NodeId,
    discovery: Arc<Discovery>,
    transport_receiver: TransportReceiver,
    transport_sender: TransportSender,
}

impl Node {
    pub async fn new(simulated_state: Arc<SimulatedState>) -> Result<Self> {
        let id = hostname::get()?.to_string_lossy().into_owned();
        let (transport_receiver, transport_sender) =
            new_transport(id.clone(), Arc::clone(&simulated_state)).await?;

        Ok(Self {
            id: id.clone(),
            discovery: Arc::new(Discovery::new(Arc::clone(&simulated_state))?),
            transport_receiver,
            transport_sender,
        })
    }

    pub async fn start(self) -> Result<()> {
        info!("Starting up node {}", self.id);

        // Periodically discover siblings
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
                if let Err(err) = discovery.discover_siblings(transport_sender.clone()).await {
                    error!("Failed to discover siblings: {}", err);
                    let target_ips: Vec<_> = {
                        let siblings = discovery.siblings.read().await;
                        siblings.values().map(|sibling| sibling.ip).collect()
                    };
                    let gossip_requests = target_ips
                        .into_iter()
                        .map(|ip| transport_sender.request_gossip(ip));
                    join_all(gossip_requests).await;
                }
            }
        });

        // Monitor inbound messages and deliver them into the channel
        let (tx, mut rx) = mpsc::channel::<Message>(32);
        let mut transport_receiver = self.transport_receiver;
        task::spawn(async move {
            loop {
                if let Some(message) = transport_receiver.get_message().await {
                    if let Err(err) = tx.send(message).await {
                        error!("Failed to send transport message into channel: {}", err);
                    }
                };
            }
        });

        // Handle inbound messages
        while let Some(message) = rx.recv().await {
            Self::handle_message(
                &message,
                self.transport_sender.clone(),
                Arc::clone(&self.discovery),
            )
            .await;
        }

        Ok(())
    }

    pub async fn handle_message(
        message: &Message,
        transport_sender: TransportSender,
        discovery: Arc<Discovery>,
    ) {
        debug!(
            "Handling {} for \"{}\" from {}{}",
            if message.direction == MessageDirection::Request {
                "request"
            } else {
                "response"
            },
            message.kind.to_string(),
            message.src_node_id,
            if let Some(payload) = message.payload.clone() {
                format!(": {}", payload)
            } else {
                "".to_string()
            },
        );

        match (&message.kind, &message.direction) {
            (MessageKind::Identity, MessageDirection::Request) => {
                if let Err(err) =
                    Self::handle_identification_request(&message, transport_sender).await
                {
                    error!("Failed to handle identification request: {}", err);
                };
            }
            (MessageKind::Identity, MessageDirection::Response) => {
                if let Err(err) = Self::handle_identification_response(&message, discovery).await {
                    error!("Failed to handle identification response: {}", err);
                };
            }
            (MessageKind::Gossip, MessageDirection::Request) => {
                if let Err(err) =
                    Self::handle_gossip_request(&message, transport_sender, discovery).await
                {
                    error!("Failed to handle gossip request: {}", err);
                };
            }
            (MessageKind::Gossip, MessageDirection::Response) => {
                if let Err(err) = Self::handle_gossip_response(&message, discovery).await {
                    error!("Failed to handle gossip response: {}", err);
                };
            }
        };
    }

    async fn handle_identification_request(
        message: &Message,
        transport_sender: TransportSender,
    ) -> Result<()> {
        let src_ip = Self::get_source_ip(message)?;
        transport_sender.respond_identification(src_ip).await?;

        Ok(())
    }

    async fn handle_identification_response(
        message: &Message,
        discovery: Arc<Discovery>,
    ) -> Result<()> {
        let src_ip = Self::get_source_ip(message)?;
        discovery
            .record_sibling(message.src_node_id.clone(), src_ip)
            .await;

        Ok(())
    }

    async fn handle_gossip_request(
        message: &Message,
        transport_sender: TransportSender,
        discovery: Arc<Discovery>,
    ) -> Result<()> {
        let siblings = discovery.siblings.read().await;
        if siblings.is_empty() {
            info!("Not aware of any siblings - ignoring gossip request");
            return Ok(());
        }

        let payload = Self::build_gossip_payload(&siblings);
        let src_ip = Self::get_source_ip(message)?;

        transport_sender.respond_gossip(src_ip, payload).await?;

        Ok(())
    }

    async fn handle_gossip_response(message: &Message, discovery: Arc<Discovery>) -> Result<()> {
        let payload = Self::get_payload(message)?;
        let siblings = Self::parse_gossip_payload(payload)?;

        discovery.record_siblings(siblings).await;

        Ok(())
    }

    fn build_gossip_payload(siblings: &SiblingsMap) -> String {
        siblings
            .iter()
            .map(|(id, sibling)| format!("{}:{}", id, sibling.ip))
            .collect::<Vec<String>>()
            .join(",")
    }

    fn parse_gossip_payload(payload: String) -> Result<SiblingsMap> {
        let siblings = payload
            .split(",")
            .map(|sibling_string| {
                let sibling_parts: Vec<&str> = sibling_string.split(":").collect();
                let id = *sibling_parts
                    .get(0)
                    .ok_or(Error::other("Missing ID in payload"))?;
                let ip = (*sibling_parts)
                    .get(1)
                    .ok_or(Error::other("Missing IP address in payload"))?
                    .parse::<IpAddr>()
                    .map_err(|e| Error::other(e))?;

                Ok((
                    id.to_string(),
                    Sibling {
                        ip,
                        last_seen: OffsetDateTime::now_utc(),
                    },
                ))
            })
            .collect::<Result<SiblingsMap>>()?;

        Ok(siblings)
    }

    fn get_source_ip(message: &Message) -> Result<IpAddr> {
        message
            .src_ip
            .ok_or_else(|| Error::other("No source IP address found"))
    }

    fn get_payload(message: &Message) -> Result<String> {
        message
            .payload
            .clone()
            .ok_or_else(|| Error::other("No payload found"))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::IpAddr};

    use time::OffsetDateTime;

    use crate::discovery::Sibling;

    use super::*;

    fn build_sibling_map() -> SiblingsMap {
        let mut siblings = HashMap::new();

        siblings.insert(
            "b62168f4fa9a".to_string(),
            Sibling {
                ip: "192.1.2.3".parse::<IpAddr>().unwrap(),
                last_seen: OffsetDateTime::now_utc(),
            },
        );
        siblings.insert(
            "c73279g5gb0b".to_string(),
            Sibling {
                ip: "192.1.2.4".parse::<IpAddr>().unwrap(),
                last_seen: OffsetDateTime::now_utc(),
            },
        );
        siblings.insert(
            "d84380h6hc1c".to_string(),
            Sibling {
                ip: "192.1.2.5".parse::<IpAddr>().unwrap(),
                last_seen: OffsetDateTime::now_utc(),
            },
        );

        siblings
    }

    #[test]
    fn it_builds_gossip_payload() {
        let siblings = build_sibling_map();
        let payload = Node::build_gossip_payload(&siblings);
        // sort to avoid flakiness from HashMap non-determinism
        let mut parts: Vec<_> = payload.split(",").collect();
        parts.sort();

        assert_eq!(
            parts,
            vec![
                "b62168f4fa9a:192.1.2.3",
                "c73279g5gb0b:192.1.2.4",
                "d84380h6hc1c:192.1.2.5"
            ],
        )
    }

    #[test]
    fn it_parses_gossip_payload() {
        let payload =
            "b62168f4fa9a:192.1.2.3,c73279g5gb0b:192.1.2.4,d84380h6hc1c:192.1.2.5".to_string();
        let expected_siblings = build_sibling_map();

        let parsed_siblings =
            Node::parse_gossip_payload(payload).expect("Expected valid payload string");

        assert_eq!(parsed_siblings, expected_siblings);
    }
}
