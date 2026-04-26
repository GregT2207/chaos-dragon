mod node;

use node::Node;

fn main() {
    let mut node = match Node::new() {
        Ok(node) => node,
        Err(err) => {
            eprintln!("Node failed to initialise: {}", err);
            return;
        }
    };

    if let Err(err) = node.start() {
        eprintln!("Node failed to start: {}", err)
    }
}
