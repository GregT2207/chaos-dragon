use std::{
    env,
    io::{Error, Result},
    net::{IpAddr, UdpSocket},
    sync::Arc,
};

use tokio::{sync::RwLock, task, time};
use trust_dns_resolver::{
    TokioAsyncResolver,
    config::{ResolverConfig, ResolverOpts},
    name_server::TokioConnectionProvider,
};

pub struct Node {
    instance_id: String,
    scanner: Arc<Scanner>,
}

struct Sibling {
    id: usize,
    ip: IpAddr,
}

struct Scanner {
    host_name: String,
    dns_resolver: TokioAsyncResolver,
    siblings: RwLock<Vec<Sibling>>,
}

impl Node {
    pub fn new() -> Result<Self> {
        let instance_id = hostname::get()?.to_string_lossy().into_owned();

        let host_name = env::var("HOST_NAME").map_err(|e| Error::other(e))?;
        let dns_resolver = TokioAsyncResolver::new(
            ResolverConfig::default(),
            ResolverOpts::default(),
            TokioConnectionProvider::default(),
        );
        let scanner = Scanner {
            host_name,
            dns_resolver,
            siblings: RwLock::new(Vec::new()),
        };

        Ok(Self {
            instance_id,
            scanner: Arc::new(scanner),
        })
    }

    // Find other sibling nodes with DNS scan
    async fn scan_siblings(scanner: Arc<Scanner>) {
        let lookup = match scanner.dns_resolver.lookup_ip(&scanner.host_name).await {
            Ok(lookup) => lookup,
            Err(_) => {
                println!("No sibling nodes found");
                return;
            }
        };

        let mut siblings = scanner.siblings.write().await;
        siblings.clear();
        for (id, ip) in lookup.iter().enumerate() {
            siblings.push(Sibling { id, ip });
        }
    }

    pub fn start(&self) -> Result<()> {
        println!("Starting up node instance {}", self.instance_id);

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

        let socket = UdpSocket::bind("127.0.0.1:3000")?;
        let mut buf = [0; 1024];
        loop {
            let (amt, src) = socket.recv_from(&mut buf)?;
            let packet = &buf[..amt];
            println!("Received a packet from {}", src.ip())
        }
    }
}
