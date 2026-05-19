use std::{
    collections::{HashMap, HashSet},
    env,
    io::{Error, Result},
    net::IpAddr,
    sync::Arc,
};

use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{sync::RwLock, task};
use trust_dns_resolver::{
    TokioAsyncResolver, name_server::TokioConnectionProvider, system_conf::read_system_conf,
};

use crate::{simulation::SimulatedState, transport::TransportSender, types::NodeId};

pub type SiblingsMap = HashMap<NodeId, Sibling>;

pub struct Discovery {
    pub siblings: RwLock<SiblingsMap>,
    sibling_expiry_time: TimeDuration,
    host_name: String,
    dns_resolver: TokioAsyncResolver,
    simulated_state: Arc<SimulatedState>,
}

#[derive(Debug)]
pub struct Sibling {
    pub ip: IpAddr,
    pub last_seen: OffsetDateTime,
}

impl PartialEq for Sibling {
    fn eq(&self, other: &Self) -> bool {
        self.ip == other.ip
    }
}

impl Discovery {
    pub fn new(simulated_state: Arc<SimulatedState>) -> Result<Self> {
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
            simulated_state,
        })
    }

    pub async fn discover_siblings(&self, transport_sender: TransportSender) -> Result<()> {
        // Remove expired sibling records
        let mut siblings = self.siblings.write().await;
        let now = OffsetDateTime::now_utc();
        siblings.retain(|_, sibling| now - sibling.last_seen < self.sibling_expiry_time);

        // Find other sibling nodes with DNS scan
        if !self.simulated_state.dns_available() {
            Error::other("DNS lookup failed");
        }
        let lookup = self.dns_resolver.lookup_ip(&self.host_name).await?;
        let discovered_ips: HashSet<IpAddr> = lookup.into_iter().collect();

        // Poll each node for identification
        for ip in discovered_ips {
            let sender = transport_sender.clone();
            task::spawn(async move { (sender.request_identification(ip).await, ip) });
        }

        Ok(())
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

    pub async fn record_siblings(&self, new_siblings: SiblingsMap) {
        let mut siblings = self.siblings.write().await;
        siblings.extend(new_siblings);

        println!("Mapping {} siblings", siblings.len());
    }
}
