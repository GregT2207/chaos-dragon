mod node;

use node::Node;

fn main() {
    let mut node = Node {};

    let started = node.start();
    if let Err(err) = started {
        println!("Node failed to start: {}", err)
    }
}
