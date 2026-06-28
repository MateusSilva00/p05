# Raft Consensus com gRPC

Implementação do algoritmo de consenso **Raft** para replicação de log entre **4 nós distribuídos**, utilizando **gRPC + Protocol Buffers** para comunicação e um **cliente Go** para demonstrar interoperabilidade.

## Arquitetura

- **4 Nós Raft** (Python) — Cada nó inicia como Follower e participa do protocolo de eleição de líder e replicação de log via gRPC.
- **Cliente Go** — REPL que descobre o líder através das respostas dos nós e publica/some dados.

## Tecnologias

- **Python 3.12** — Servidores Raft
- **Go** — Cliente (demonstrando interoperabilidade do gRPC)
- **gRPC + Protocol Buffers** — Toda a comunicação
- **Pydantic v2** — Modelagem de domínio e validação
- **Loguru** — Logging estruturado
- **uv** — Gerenciador de pacotes Python
- **Docker / Docker Compose** — Orquestração de containers

## Como Executar

### Pré-requisitos

- Docker e Docker Compose

### Iniciar o cluster

```bash
./run.sh
```

Isso inicializa:
1. **4 nós Raft** na porta interna `50051` cada
2. **Container do cliente Go** (interativo)

### Usar o cliente

```bash
docker attach client
```

Comandos:
- `publish <data>` — Escreve dados no cluster
- `consume` — Lê todos os dados efetivados (committed)
- `exit` — Sair

### Cenários de demonstração

```bash
# Cenário 2 — Falha do Líder
docker stop node_X    # (substitua X pelo líder)

# Cenário 3/4 — Persistência e recuperação
docker stop node_X
docker start node_X

# Parar tudo
docker compose down

# Parar e excluir dados persistidos
docker compose down --volumes
```
