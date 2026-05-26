use log::info;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::sleep;

pub struct Simulation {
    rng: StdRng,
    pub state: Arc<SimulatedState>,
}

pub struct SimulatedState {
    inbound_network_messages_available: AtomicBool,
    outbound_network_messages_available: AtomicBool,
    dns_available: AtomicBool,
}

impl Simulation {
    pub fn new() -> Self {
        Simulation {
            rng: StdRng::from_rng(&mut rand::rng()),
            state: Arc::new(SimulatedState {
                inbound_network_messages_available: AtomicBool::new(true),
                outbound_network_messages_available: AtomicBool::new(true),
                dns_available: AtomicBool::new(true),
            }),
        }
    }

    pub async fn start(&mut self) {
        info!("Starting chaos simulation");
        loop {
            sleep(Duration::from_secs(self.rng.random_range(8..30))).await; // random frequency
            let failure_seconds = self.rng.random_range(1..=30); // random duration

            match self.rng.random_range(0..=3) {
                0 => {
                    self.run_simulation(
                        &self.state.inbound_network_messages_available,
                        failure_seconds,
                        "inbound network failure",
                    )
                    .await
                }
                1 => {
                    self.run_simulation(
                        &self.state.outbound_network_messages_available,
                        failure_seconds,
                        "outbound network failure",
                    )
                    .await
                }
                2 => {
                    self.run_simulation(&self.state.dns_available, failure_seconds, "DNS failure")
                        .await
                }
                _ => info!("It's your lucky day, punk"),
            }
        }
    }

    async fn run_simulation(
        &self,
        simulated_field: &AtomicBool,
        failure_seconds: u64,
        display_name: &str,
    ) {
        info!(
            "Simulating {} for {} seconds",
            display_name, failure_seconds
        );

        simulated_field.store(false, Ordering::Relaxed);
        sleep(Duration::from_secs(failure_seconds)).await;
        simulated_field.store(true, Ordering::Relaxed);
    }
}

impl SimulatedState {
    pub fn inbound_network_messages_available(&self) -> bool {
        self.inbound_network_messages_available
            .load(Ordering::Relaxed)
    }

    pub fn outbound_network_messages_available(&self) -> bool {
        self.outbound_network_messages_available
            .load(Ordering::Relaxed)
    }

    pub fn dns_available(&self) -> bool {
        self.dns_available.load(Ordering::Relaxed)
    }
}
