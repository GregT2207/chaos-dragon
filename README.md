# Chaos Dragon

## Description

Chaos Dragon is an educational project that runs multiple nodes on an internal network that communicate to cooperatively recover from simulated failures.

The goal of the simulations is to stress test the system's ability to:

- never crash
- log descriptively and selectively for excellent observability
- minimise pressure on failing services through throttling and exponential backoff
- maximise delivery speed by maintaining the most current view of the network possible

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

## Preview
<img width="743" height="720" alt="Screenshot 2026-05-26 at 22 54 19" src="https://github.com/user-attachments/assets/ad33cb53-0a23-4136-8bea-80fe43b08a57" />
