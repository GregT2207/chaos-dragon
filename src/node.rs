use std::{
    collections::HashMap,
    env,
    io::{Error, ErrorKind, Result},
    net::{IpAddr, UdpSocket},
    str::from_utf8,
    sync::Arc,
};

use tokio::{sync::RwLock, task, time};
use trust_dns_resolver::{
    TokioAsyncResolver, name_server::TokioConnectionProvider, system_conf::read_system_conf,
};

pub struct Node {
    instance_id: String,
    scanner: Arc<Scanner>,
}

type NodeId = usize;
struct SiblingNode {
    ip: IpAddr,
}

struct Scanner {
    host_name: String,
    dns_resolver: TokioAsyncResolver,
    siblings: RwLock<HashMap<NodeId, SiblingNode>>,
}

struct Message<'a> {
    target_node_id: NodeId,
    payload: &'a str,
    src_ip: IpAddr,
}

impl Node {
    pub fn new() -> Result<Self> {
        let instance_id = hostname::get()?.to_string_lossy().into_owned();

        let host_name = env::var("HOST_NAME").map_err(|e| Error::other(e))?;
        let (config, opts) = read_system_conf()?;
        let dns_resolver =
            TokioAsyncResolver::new(config, opts, TokioConnectionProvider::default());
        let scanner = Scanner {
            host_name,
            dns_resolver,
            siblings: RwLock::new(HashMap::new()),
        };

        Ok(Self {
            instance_id,
            scanner: Arc::new(scanner),
        })
    }

    pub async fn start(&self) -> Result<()> {
        println!("Starting up node instance {}", self.instance_id);

        self.spawn_scan_siblings()?;

        let socket = UdpSocket::bind("0.0.0.0:3000")?;
        let mut buf = [0; 1024];
        loop {
            let (amt, src) = socket.recv_from(&mut buf)?;
            let message = Self::parse_message(&buf[..amt], src.ip());
            match message {
                Ok(message) => self.handle_message(message).await,
                Err(err) => eprintln!("Failed to parse message: {}", err),
            }
        }
    }

    fn spawn_scan_siblings(&self) -> Result<()> {
        let interval_ms = env::var("SCAN_SIBLINGS_INTERVAL_MS")
            .map_err(|e| Error::other(e))?
            .parse::<u64>()
            .map_err(|e| Error::other(e))?;
        let mut interval = time::interval(time::Duration::from_millis(interval_ms));

        let scanner = Arc::clone(&self.scanner);
        task::spawn(async move {
            loop {
                interval.tick().await;
                Node::scan_siblings(Arc::clone(&scanner)).await;
            }
        });

        Ok(())
    }

    // Find other sibling nodes with DNS scan
    async fn scan_siblings(scanner: Arc<Scanner>) {
        let lookup = match scanner.dns_resolver.lookup_ip(&scanner.host_name).await {
            Ok(lookup) => lookup,
            Err(_) => {
                eprintln!("No sibling nodes found");
                return;
            }
        };

        let mut siblings = scanner.siblings.write().await;
        siblings.clear();
        for (id, ip) in lookup.iter().enumerate() {
            siblings.insert(id, SiblingNode { ip });
        }
        println!("Found {} sibling nodes", siblings.len())
    }

    fn parse_message<'a>(bytes: &'a [u8], ip: IpAddr) -> Result<Message<'a>> {
        let raw_text = Self::parse_message_get_raw_text(&bytes)?;
        let (sibling_node_id, payload) = Self::parse_message_split_raw_text(&raw_text)?;

        Ok(Message {
            target_node_id: sibling_node_id,
            payload,
            src_ip: ip,
        })
    }

    fn parse_message_get_raw_text(bytes: &[u8]) -> Result<&str> {
        from_utf8(bytes).map_err(|e| {
            Error::other(format!(
                "Failed to parse incoming message as UTF-8 text: {}",
                e
            ))
        })
    }

    fn parse_message_split_raw_text(raw_text: &str) -> Result<(NodeId, &str)> {
        let mut sibling_node_id: Option<&str> = None;
        let mut payload: Option<&str> = None;

        let split = raw_text.split('|');
        for (index, part) in split.enumerate() {
            if index == 0 {
                sibling_node_id = Some(part);
            } else if index == 1 {
                payload = Some(part);
            } else {
                println!(
                    "Warning: incoming message contained more parts than expected, ignoring additional parts"
                )
            }
        }

        match (sibling_node_id, payload) {
            (Some(sibling_node_id), Some(payload)) => {
                let sibling_node_id = sibling_node_id.parse::<NodeId>().map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("Failed to parse sibling ID: {}", e),
                    )
                })?;

                return Ok((sibling_node_id, payload));
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Did not contain both expected parts",
                ));
            }
        }
    }

    async fn handle_message<'a>(&self, message: Message<'a>) {
        let siblings = self.scanner.siblings.read().await;
        let target_sibling = siblings.get(&message.target_node_id);

        match target_sibling {
            Some(target_sibling) => println!(
                "Received a message {} from {}, intended for {} on {}",
                message.payload, message.src_ip, message.target_node_id, target_sibling.ip,
            ),
            None => println!("Target sibling not found, dropping message"),
        }
    }
}
