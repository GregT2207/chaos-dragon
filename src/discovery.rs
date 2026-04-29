use std::{
    collections::{HashMap, HashSet},
    env,
    io::{Error, Result},
    net::IpAddr,
};

use futures::future::join_all;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{sync::RwLock, task};
use trust_dns_resolver::{
    TokioAsyncResolver, name_server::TokioConnectionProvider, system_conf::read_system_conf,
};

use crate::{transport::Transport, types::NodeId};

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

    pub async fn discover_siblings(&self) {
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
        let poll_handles = discovered_ips.into_iter().map(|ip| {
            task::spawn(async move { (Transport::request_identification(&ip).await, ip) })
        });
        let poll_responses = join_all(poll_handles).await;

        // Update siblings with refreshed timestamp
        let mut siblings = self.siblings.write().await;
        for response in poll_responses {
            if let Ok(response) = response {
                siblings.insert(
                    response.0,
                    Sibling {
                        ip: response.1,
                        last_seen: OffsetDateTime::now_utc(),
                    },
                );
            }
        }

        // Remove expired sibling records
        let now = OffsetDateTime::now_utc();
        siblings.retain(|_, sibling| now - sibling.last_seen < self.sibling_expiry_time);
    }
}
