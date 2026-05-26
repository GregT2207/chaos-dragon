mod backoff;
mod discovery;
mod node;
mod simulation;
mod transport;
mod types;

use crate::simulation::Simulation;
use log::error;
use node::Node;
use rand::RngExt;
use std::{sync::Arc, time::Duration};
use tokio::{task, time::sleep};

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    // Simulates nodes progressively coming online
    let mut rng = rand::rng();
    sleep(Duration::from_millis(rng.random_range(1000..30000))).await;

    let mut simulation = Simulation::new();

    let node = match Node::new(Arc::clone(&simulation.state)).await {
        Ok(node) => node,
        Err(err) => {
            error!("Failed to initialise node: {}", err);
            return;
        }
    };

    task::spawn(async move {
        simulation.start().await;
    });

    if let Err(err) = node.start().await {
        error!("Failed to start node: {}", err)
    }
}
