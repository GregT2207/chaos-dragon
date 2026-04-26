use std::{
    env,
    io::{Error, Result},
    net::{IpAddr, UdpSocket},
};

use trust_dns_resolver::{
    Resolver,
    config::{ResolverConfig, ResolverOpts},
};

pub struct Node {
    instance_id: String,
    host_name: String,
    dns_resolver: Resolver,
    siblings: Vec<Sibling>,
}

struct Sibling {
    id: usize,
    ip: IpAddr,
}

impl Node {
    // Identify
    pub fn new() -> Result<Self> {
        let instance_id = hostname::get()?.to_string_lossy().into_owned();
        let host_name = env::var("HOST_NAME").map_err(|e| Error::other(e))?;
        let dns_resolver = Resolver::new(ResolverConfig::default(), ResolverOpts::default())?;

        Ok(Self {
            instance_id,
            host_name,
            dns_resolver,
            siblings: Vec::new(),
        })
    }

    // Find other sibling nodes with DNS scan
    fn scan_siblings(&mut self) -> Result<()> {
        let lookup = match self.dns_resolver.lookup_ip(&self.host_name) {
            Ok(lookup) => lookup,
            Err(_) => {
                println!("No sibling nodes found");
                return Ok(());
            }
        };

        self.siblings.clear();
        for (id, ip) in lookup.iter().enumerate() {
            self.siblings.push(Sibling { id, ip });
        }

        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        println!("Starting up node instance {}", self.instance_id);

        self.scan_siblings()?;

        let socket = UdpSocket::bind("127.0.0.1:3000")?;
        let mut buf = [0; 1024];
        loop {
            let (amt, src) = socket.recv_from(&mut buf)?;
            let packet = &buf[..amt];
            println!("Received a packet from {}", src.ip())
        }
    }
}
