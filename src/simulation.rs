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
    dns_available: AtomicBool,
}

impl Simulation {
    pub fn new() -> Self {
        Simulation {
            rng: StdRng::from_rng(&mut rand::rng()),
            state: Arc::new(SimulatedState {
                dns_available: AtomicBool::new(true),
            }),
        }
    }

    pub async fn start(&mut self) {
        info!("Starting chaos simulation");
        loop {
            sleep(Duration::from_secs(self.rng.random_range(3..30))).await;

            match self.rng.random_range(0..=1) {
                0 => self.dns_failure().await,
                _ => info!("It's your lucky day, punk"),
            }
        }
    }

    async fn dns_failure(&mut self) {
        if !self.state.dns_available() {
            return;
        }

        let failure_seconds = self.rng.random_range(1..=10);
        info!("DNS failure for {} seconds", failure_seconds);

        self.state.dns_available.store(false, Ordering::Relaxed);
        sleep(Duration::from_secs(failure_seconds)).await;
        self.state.dns_available.store(true, Ordering::Relaxed);
    }
}

impl SimulatedState {
    pub fn dns_available(&self) -> bool {
        self.dns_available.load(Ordering::Relaxed)
    }
}
