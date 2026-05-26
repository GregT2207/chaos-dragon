# Chaos Dragon

## Description

Chaos Dragon is an educational project that runs multiple nodes on an internal network that can communicate to cooperatively recover from simulated failures.

## Architecture

- **Docker** network to support DNS-based discovery between scaled peer node instances
- Lightweight plain-text protocol over UDP for asynchronous request/response messages
- Peer node application written in **Rust** with the **Tokio** async runtime

## Instructions

- Clone the repository and install Docker
- Enter the folder, create the environment variables file, and optionally configure them
  - `cd chaos-dragon && cp .env.example .env`
- Use Docker Compose to launch multiple instances of the peer nodes
  - `docker compose up`
- Monitor the logs to see simulated failures and node recovery behaviour
