use std::{
    collections::{HashMap, HashSet},
    env,
    io::{Error, ErrorKind, Result},
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{sync::RwLock, task, time::sleep};
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
    dns_backoff_seconds: AtomicU64,
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
            dns_backoff_seconds: AtomicU64::new(0),
            simulated_state,
        })
    }

    pub async fn discover_siblings(&self, transport_sender: TransportSender) -> Result<()> {
        self.remove_expired_siblings().await;

        // Check simulated DNS availability
        if !self.simulated_state.dns_available() {
            let dns_backoff_seconds = self.dns_backoff_seconds.load(Ordering::Relaxed);
            if dns_backoff_seconds > 0 {
                println!(
                    "Waiting {} seconds before attempting another DNS scan",
                    dns_backoff_seconds
                );
                sleep(Duration::from_secs(dns_backoff_seconds)).await;
                self.dns_backoff_seconds
                    .store(dns_backoff_seconds * 2, Ordering::Relaxed);
            } else {
                self.dns_backoff_seconds.store(2, Ordering::Relaxed);
            }

            return Err(Error::new(ErrorKind::NotFound, "DNS lookup failed"));
        }
        self.dns_backoff_seconds.store(0, Ordering::Relaxed);

        self.poll_and_identify_siblings(transport_sender).await
    }

    async fn remove_expired_siblings(&self) {
        let mut siblings = self.siblings.write().await;
        let now = OffsetDateTime::now_utc();
        siblings.retain(|_, sibling| now - sibling.last_seen < self.sibling_expiry_time);
    }

    async fn poll_and_identify_siblings(&self, transport_sender: TransportSender) -> Result<()> {
        let siblings = self.siblings.read().await;
        let lookup = self.dns_resolver.lookup_ip(&self.host_name).await?;
        let discovered_ips: HashSet<IpAddr> = lookup.into_iter().collect();

        let existing_ips: HashSet<IpAddr> = siblings.values().map(|sibling| sibling.ip).collect();
        for ip in discovered_ips.difference(&existing_ips) {
            let ip = ip.clone();
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
