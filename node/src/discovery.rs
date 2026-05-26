use std::{
    collections::{HashMap, HashSet},
    env,
    io::{Error, ErrorKind, Result},
    net::IpAddr,
    sync::Arc,
    time::Duration,
};

use futures::lock::Mutex;
use log::info;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{sync::RwLock, task, time::sleep};
use trust_dns_resolver::{
    TokioAsyncResolver, name_server::TokioConnectionProvider, system_conf::read_system_conf,
};

use crate::{
    backoff::ExponentialBackoff, simulation::SimulatedState, transport::TransportSender,
    types::NodeId,
};

pub type SiblingsMap = HashMap<NodeId, Sibling>;

pub struct Discovery {
    pub siblings: RwLock<SiblingsMap>,
    sibling_expiry_time: TimeDuration,
    host_name: String,
    dns_resolver: TokioAsyncResolver,
    backoff: Mutex<ExponentialBackoff>,
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
            backoff: Mutex::new(ExponentialBackoff::new()),
            simulated_state,
        })
    }

    pub async fn discover_siblings(&self, transport_sender: TransportSender) -> Result<()> {
        // Sibling records won't be removed until DNS is available and new poll succeeds
        self.check_network_backoff().await?;
        self.poll_and_identify_siblings(transport_sender).await?;
        self.remove_expired_siblings().await;

        Ok(())
    }

    async fn check_network_backoff(&self) -> Result<()> {
        let remaining_wait: Duration;
        {
            let mut backoff = self.backoff.lock().await;

            if self.simulated_state.outbound_network_messages_available()
                && self.simulated_state.dns_available()
            {
                backoff.reset();
                return Ok(());
            }

            remaining_wait = backoff.remaining_wait();
        }

        sleep(remaining_wait).await;
        return Err(Error::new(ErrorKind::NotFound, "DNS unavailable"));
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

        info!("Aware of {} siblings", siblings.len());
        Ok(())
    }

    pub async fn record_sibling(&self, sibling_id: NodeId, sibling_ip: IpAddr) {
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
        let mut existing_siblings = self.siblings.write().await;

        for (new_sibling_key, new_sibling_value) in new_siblings {
            // Don't update an existing record if it was seen more recently
            if let Some(existing_sibling) = existing_siblings.get(&new_sibling_key)
                && existing_sibling.last_seen > new_sibling_value.last_seen
            {
                continue;
            }

            existing_siblings.insert(new_sibling_key, new_sibling_value);
        }
    }
}
