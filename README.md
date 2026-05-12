# Chaos Dragon

Chaos Dragon is an educational project that runs multiple nodes on an internal network that can communicate to cooperatively recover from simulated failures.

## Architecture

Peer node application written in Rust, using the Tokio runtime for reliable asynchronous communication, running inside a Docker network. DNS for peer discovery and a custom plain-text protocol over UDP for simple request-response messages.

## Current Status: WIP
