use std::{
    collections::{HashMap, HashSet},
    env,
    io::{Error, Result},
    net::IpAddr,
};

use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{sync::RwLock, task};
use trust_dns_resolver::{
    TokioAsyncResolver, name_server::TokioConnectionProvider, system_conf::read_system_conf,
};

use crate::{transport::TransportSender, types::NodeId};

pub struct Discovery {
    pub siblings: RwLock<HashMap<NodeId, Sibling>>,
    sibling_expiry_time: TimeDuration,
    host_name: String,
    dns_resolver: TokioAsyncResolver,
}

pub struct Sibling {
    pub ip: IpAddr,
    last_seen: OffsetDateTime,
}

impl Discovery {
    pub fn new() -> Result<Self> {
        let sibling_expiry_time_ms = env::var("SIBLING_EXPIRY_TIME_MS")
            .map_err(|e| Error::other(e))?
            .parse::<i64>()
            .map_err(|e| Error::other(e))?;
        let (config, opts) = read_system_conf()?;

        Ok(Self {
            siblings: RwLock::new(HashMap::new()),
            sibling_expiry_time: TimeDuration::milliseconds(sibling_expiry_time_ms),
            host_name: env::var("HOST_NAME").map_err(|e| Error::other(e))?,
            dns_resolver: TokioAsyncResolver::new(config, opts, TokioConnectionProvider::default()),
        })
    }

    pub async fn discover_siblings(&self, transport_sender: TransportSender) {
        // Remove expired sibling records
        let mut siblings = self.siblings.write().await;
        let now = OffsetDateTime::now_utc();
        siblings.retain(|_, sibling| now - sibling.last_seen < self.sibling_expiry_time);

        // Find other sibling nodes with DNS scan
        let lookup = match self.dns_resolver.lookup_ip(&self.host_name).await {
            Ok(lookup) => lookup,
            Err(_) => {
                eprintln!("No sibling nodes found");
                return;
            }
        };
        let discovered_ips: HashSet<IpAddr> = lookup.into_iter().collect();

        // Poll each node for identification
        for ip in discovered_ips {
            let sender = transport_sender.clone();
            task::spawn(async move { (sender.request_identification(ip).await, ip) });
        }
    }

    pub async fn record_sibling(&self, sibling_id: NodeId, sibling_ip: IpAddr) {
        println!("Mapping sibling {} to {}", &sibling_id, &sibling_ip);

        let mut siblings = self.siblings.write().await;
        siblings.insert(
            sibling_id,
            Sibling {
                ip: sibling_ip,
                last_seen: OffsetDateTime::now_utc(),
            },
        );
    }
}
