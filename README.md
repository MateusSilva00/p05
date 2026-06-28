# ⚔️ Coliseum — Raft Consensus with gRPC

Implementation of the **Raft** consensus algorithm for replicated log across **4 distributed nodes**, using **gRPC + Protocol Buffers** for communication and a **Go client** for interoperability.

## 📐 Architecture

- **4 Raft Nodes** (Python) — Each starts as Follower and participates in leader election and log replication via gRPC.
- **Go Client** — REPL that discovers the leader through node responses and publishes/consumes data.

## 🛠️ Technologies

- **Python 3.12** — Raft servers
- **Go** — Client (demonstrating gRPC interoperability)
- **gRPC + Protocol Buffers** — All communication
- **Pydantic v2** — Domain models and validation
- **Loguru** — Structured logging
- **uv** — Python package manager
- **Docker / Docker Compose** — Container orchestration

## 🚀 How to Run

### Prerequisites

- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)

### Start the cluster

```bash
./run.sh
```

This starts:
1. **4 Raft nodes** on internal port `50051` each
2. **Go client** container (interactive)

### Use the client

```bash
docker attach client
```

Commands:
- `publish <data>` — Write data to the cluster
- `consume` — Read all committed data
- `exit` — Quit

### Demo scenarios

```bash
# Scenario 2 — Leader failure
docker stop node_X    # (replace X with the leader)

# Scenario 3/4 — Persistence and recovery
docker stop node_X
docker start node_X

# Stop everything
docker compose down

# Stop and delete persisted data
docker compose down --volumes
```
