mod discovery;
mod node;
mod transport;
mod types;

use node::Node;

#[tokio::main]
async fn main() {
    let node = match Node::new() {
        Ok(node) => node,
        Err(err) => {
            eprintln!("Node failed to initialise: {}", err);
            return;
        }
    };

    if let Err(err) = node.start().await {
        eprintln!("Node failed to start: {}", err)
    }
}
