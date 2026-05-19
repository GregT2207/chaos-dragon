mod discovery;
mod node;
mod simulation;
mod transport;
mod types;

use std::time::Duration;

use node::Node;
use rand::RngExt;
use tokio::{task, time::sleep};

use crate::simulation::Simulation;

#[tokio::main]
async fn main() {
    let mut rng = rand::rng();
    sleep(Duration::from_millis(rng.random_range(1000..5000))).await;

    let mut simulation = Simulation::new();

    let node = match Node::new(simulation.state.clone()).await {
        Ok(node) => node,
        Err(err) => {
            eprintln!("Node failed to initialise: {}", err);
            return;
        }
    };

    task::spawn(async move {
        simulation.start().await;
    });

    if let Err(err) = node.start().await {
        eprintln!("Node failed to start: {}", err)
    }
}
