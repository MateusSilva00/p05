# 🧠 Agent Context — P05 Raft gRPC

> **RTK**: Always read this file before any prompt to save tokens and avoid re-research.

---

## 📋 Project Understanding

### What is this?
- **Course**: Sistemas Distribuídos — UTFPR (2026.1)
- **Assignment**: P05 — Evolve an existing **Pyro5-based Raft** implementation to **gRPC + Protocol Buffers**
- **Value**: 20 points total

### What already exists (Pyro5 version)
- **4 Raft nodes** running in Docker containers (docker-compose.yml has 6, but assignment says 4)
- **Pyro5 nameserver** for service discovery
- **Python REPL client** (same language as servers — must change to **another language** in gRPC version)
- Core models: `RaftNode` (Pydantic), `NodeState` enum, `LogEntry` as plain dicts
- Election: `ElectionManager` — parallel vote requests via threads + Pyro5 proxies
- Heartbeat: `HeartbeatManager` — parallel AppendEntries (currently sends empty entries only)
- Server: `RaftServer` — tick loop checking election/heartbeat timeouts
- Client: `RaftClient` — REPL that discovers leader via Pyro5 nameserver

### What must change (Pyro5 → gRPC)
All communication via **gRPC** using `.proto` definitions. No more Pyro5.

---

## 🎯 Assignment Requirements & Points

| Feature | Points | Status |
|---|---|---|
| **Client in different language** + leader discovery via node responses | 3.0 | ❌ |
| **Election** (random timeout, 1 vote/term, log up-to-date check) | 2.0 | ❌ |
| **Read operations** (leader + replicas, only committed data) | 3.0 | ❌ |
| **AppendEntries** (heartbeat, replication, prevLogIndex/prevLogTerm, per-replica sync, quorum=majority of 4) | 5.0 | ❌ |
| **Persistence** (term, vote, uncommitted log, committed log) | 4.0 | ❌ |
| **Recovery & Reintegration** (restore state, rejoin cluster, find leader) | 3.0 | ❌ |

### Key rules
- **4 nodes** (not 6)
- **Quorum** = majority of 4 = **3 nodes**
- Quorum is based on **total cluster config**, NOT on currently alive nodes
- No full DB sync — only missing entries via AppendEntries
- Only **committed** data can be returned to client
- Client must discover leader **through node responses** (not via external registry)
- Client in a **different language** (e.g., Go, TypeScript/Node, Rust)

### Demo Scenarios
1. Normal operation (init → election → write → replicate → read)
2. Leader failure (kill leader → new election → continue ops)
3. Persistence (stop node → restart → state recovered)
4. Replica recovery (stop replica → new writes → restart → sync only missing)
5. Read consistency (read from leader + replicas → only committed data)

---

## 🔧 Workflow Rules

### Approach: BOTTOM-UP
Build from the lowest-level abstractions upward.

### ⚠️ CRITICAL RULE
- **NEVER write code directly to the editor/files**
- Show code **ONLY in the agent chat panel**
- User evaluates, modifies, and inserts the code themselves
- Agent role = explain + demonstrate in chat, NOT auto-write

### For EACH step:
1. **Explain the concept** (theory + why it matters)
2. **Show the code in the chat panel** (professional-grade Python: uv, pydantic, classes, type hints, SoC)
3. **Suggest a commit message** (in English, conventional commits)
4. **Wait for user approval** before proceeding

### Client language: **Go**

### Code quality standards
- **uv** for package management
- **Pydantic v2** for all data models
- **Type hints** everywhere
- **Classes** with clear responsibilities
- **Separation of Concerns** — each module does one thing
- **Modern Python** (3.12+, `|` unions, `StrEnum`, etc.)

### Communication
- **RTK** (Reduce Token Konsumption): Be concise, don't repeat what's already documented here
- Explain in **Portuguese** (user's language), code in **English**
- Don't dump all code at once — step by step

---

## 🏗️ Planned Bottom-Up Architecture

### Layer 0 — Proto definitions
- `raft.proto` — internal Raft RPCs (RequestVote, AppendEntries)
- `kv.proto` — client-facing RPCs (Publish, Consume, with leader redirect)

### Layer 1 — Domain models (Pydantic)
- `LogEntry` — term, index, command
- `NodeState` — Follower, Candidate, Leader (StrEnum)
- `PersistentState` — term, voted_for, log (saved to disk)
- `VolatileState` — commit_index, last_applied, next_index[], match_index[]

### Layer 2 — Persistence
- JSON/SQLite storage for PersistentState
- Load on startup, save on every state mutation

### Layer 3 — Raft core logic (pure, no I/O)
- Vote granting logic
- AppendEntries handling (with prevLogIndex/prevLogTerm)
- Commit index advancement
- State transitions

### Layer 4 — gRPC service layer
- `RaftServiceServicer` — handles incoming RPCs
- `RaftServiceStub` wrapper — sends RPCs to peers

### Layer 5 — Server orchestration
- Election timer, heartbeat timer
- Leader replication loop (per-follower next_index tracking)

### Layer 6 — Client (different language)
- Likely Go or TypeScript
- Connects to any node → gets redirected to leader
- Publish/Consume operations

### Layer 7 — Docker & Compose
- 4 nodes, no nameserver needed (gRPC uses direct addresses)
- Persistent volumes for state files

---

## 📁 Target File Structure

```
p05/
├── proto/
│   ├── raft.proto          # Internal Raft RPCs
│   └── kv.proto            # Client-facing RPCs
├── src/
│   ├── core/
│   │   ├── models.py       # Pydantic domain models
│   │   ├── persistence.py  # State persistence
│   │   ├── config.py       # Cluster configuration
│   │   └── log.py          # Log management
│   ├── server/
│   │   ├── raft_service.py # gRPC servicer (incoming RPCs)
│   │   ├── peer_client.py  # gRPC stub wrapper (outgoing RPCs)
│   │   ├── election.py     # Election logic
│   │   ├── replication.py  # AppendEntries / heartbeat logic
│   │   └── server.py       # Orchestrator
│   └── generated/          # protoc output (gitignored or committed)
├── client/                 # Client in Go/TS/etc.
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
└── run.sh
```
