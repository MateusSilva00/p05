# 📚 Raft Consensus — Teoria e Guia do Projeto

## 1. O que é o Raft?

Raft é um algoritmo de **consenso distribuído** projetado para ser compreensível.
Ele garante que múltiplos nós concordem sobre uma sequência de operações, mesmo
quando nós falham ou a rede é instável.

**Problema que resolve**: em sistemas distribuídos, como garantir que todos os
nós vejam a mesma sequência de dados (log), mesmo com falhas?

**Resposta**: eleger um **líder** que coordena a replicação. Se o líder cai,
uma nova eleição elege outro.

---

## 2. Conceitos Fundamentais

### 2.1 — Papéis (Roles)

Cada nó está em **exatamente um** papel a qualquer momento:

| Papel | Responsabilidade |
|---|---|
| **Follower** | Passivo — responde a RPCs do líder e candidatos |
| **Candidate** | Em eleição — solicita votos dos peers |
| **Leader** | Ativo — recebe escritas do cliente e replica para followers |

**Transições**:
```
Follower → (timeout expira) → Candidate
Candidate → (maioria dos votos) → Leader
Candidate → (termo maior recebido) → Follower
Leader → (termo maior recebido) → Follower
```

### 2.2 — Termos (Terms)

O tempo no Raft é dividido em **termos** (mandatos). Cada termo tem no máximo
um líder. O termo é um **inteiro monotonicamente crescente**.

- Quando um nó vê um termo maior que o seu, ele atualiza e vira Follower.
- Quando um nó recebe uma mensagem com termo menor, ele rejeita.

### 2.3 — Log Replicado

O log é uma **sequência ordenada** de entradas `(term, index, command)`.
O líder recebe comandos do cliente, cria entradas e as replica via AppendEntries.

**Committed** = maioria dos nós confirmou a entrada → pode ser aplicada.
**Uncommitted** = ainda não tem quórum → não pode ser retornada ao cliente.

### 2.4 — Quórum

Para um cluster de **N = 4** nós, a maioria é **⌊N/2⌋ + 1 = 3**.
O quorum é calculado sobre o **tamanho total do cluster**, nunca sobre quantos
nós estão online no momento.

---

## 3. RPCs do Raft

### 3.1 — RequestVote (Eleição)

Disparada quando o election timeout expira:

1. Nó incrementa termo, vota em si mesmo, vira Candidate
2. Envia `RequestVote(term, candidate_id, last_log_index, last_log_term)` aos peers
3. Peer concede voto se:
   - Termo do candidato ≥ seu
   - Não votou em outro neste termo
   - Log do candidato está **atualizado** (comparação por `last_log_term` e depois `last_log_index`)
4. Se candidato recebe maioria → Leader
5. Se recebe termo maior → Follower

### 3.2 — AppendEntries (Heartbeat + Replicação)

Enviado periodicamente pelo líder:

1. Se `entries` está vazio → é **heartbeat** (reseta election timeout do follower)
2. Se `entries` tem dados → é **replicação**
3. Follower verifica `prev_log_index` e `prev_log_term`:
   - Match → aceita e aplica as entradas
   - Mismatch → rejeita com `conflict_index` (hint para o líder recuar mais rápido)
4. Líder rastreia `next_index` e `match_index` **por follower**
5. Quando maioria tem `match_index >= N` → `commit_index` avança para N

### 3.3 — Publish / Consume (Cliente)

- **Publish**: cliente envia dado ao líder. Se o nó não é líder, retorna
  `leader_id` para redirect. Líder cria entrada e replica. Só responde OK após commit.
- **Consume**: retorna dados committed. Funciona em **qualquer nó** (líder ou follower).

---

## 4. Persistência

O Raft exige que 3 coisas sobrevivam a crashes:

| Dado | Por quê |
|---|---|
| `current_term` | Para não votar duas vezes no mesmo termo |
| `voted_for` | Mesma razão |
| `log[]` | Para não perder entradas replicadas |

Neste projeto, usamos **JSON** em `/app/data/{node_id}.json`.
`StatePersistence.save()` é chamado após **toda mutação** de estado persistente.

`commit_index` e `last_applied` são **voláteis** — reconstruídos ao receber
AppendEntries do líder após reiniciar.

---

## 5. Recuperação de Falhas

Quando um nó reinicia:

1. Carrega `PersistentState` do disco (termo, voto, log)
2. Inicia como Follower com `commit_index = 0`
3. Ao receber AppendEntries do líder:
   - Descobre quem é o líder (`leader_id` na mensagem)
   - Sincroniza entradas faltantes (líder detecta gap via `next_index`)
   - Atualiza `commit_index` via `leader_commit`

O líder envia **apenas as entradas ausentes** daquela réplica, nunca o log inteiro.

---

## 6. gRPC e Protocol Buffers

### O que é gRPC?

Framework de RPC (Remote Procedure Call) do Google:
- Definição de API via `.proto` (IDL agnóstica de linguagem)
- Serialização binária com Protocol Buffers (mais rápido que JSON)
- HTTP/2 com multiplexação de streams
- Geração automática de código para qualquer linguagem

### Por que gRPC no Raft?

1. **Tipagem forte** — mensagens com schema, não dicts aleatórios
2. **Interoperabilidade** — servidor Python, cliente Go, tudo compatível
3. **Performance** — binário e HTTP/2, ideal para heartbeats frequentes

### Geração de código

```bash
# Python
uv run python -m grpc_tools.protoc \
  -I src/proto \
  --python_out=src/generated \
  --grpc_python_out=src/generated \
  src/proto/raft.proto

# Fix imports
sed -i 's/^import raft_pb2 as raft__pb2$/from . import raft_pb2 as raft__pb2/' \
  src/generated/raft_pb2_grpc.py

# Go
protoc -I src/proto \
  --go_out=client/pb --go_opt=paths=source_relative \
  --go-grpc_out=client/pb --go-grpc_opt=paths=source_relative \
  src/proto/raft.proto
```

---

## 7. Arquitetura do Projeto

```
p05/
├── src/
│   ├── proto/raft.proto        # Contrato gRPC (source of truth)
│   ├── generated/              # Stubs gerados (raft_pb2.py, raft_pb2_grpc.py)
│   ├── core/
│   │   ├── models.py           # PersistentState, VolatileState, LeaderState, LogEntry
│   │   ├── config.py           # Endereços dos peers, timeouts, quorum
│   │   ├── persistence.py      # JSON persistence (save/load)
│   │   └── logging.py          # Loguru config
│   └── server/
│       ├── raft_node.py        # State machine (vote, append, commit) — zero I/O
│       ├── election.py         # Parallel RequestVote via gRPC
│       ├── replication.py      # Parallel AppendEntries via gRPC
│       ├── server.py           # gRPC server + servicers + tick loop
│       └── init_server.py      # Entrypoint
├── client/
│   ├── main.go                 # Go REPL client
│   ├── go.mod / go.sum
│   └── pb/                     # Generated Go stubs
├── Dockerfile                  # Python server image
├── Dockerfile.client           # Go client image
├── docker-compose.yml          # 4 nodes + client
└── run.sh                      # One-command startup
```

### Separation of Concerns

| Camada | Arquivo | Responsabilidade |
|---|---|---|
| **Domain** | `models.py` | Estruturas de dados puras |
| **Persistence** | `persistence.py` | I/O de disco |
| **State Machine** | `raft_node.py` | Lógica do Raft (sem rede) |
| **Network** | `election.py`, `replication.py` | RPCs de saída |
| **Service** | `server.py` | RPCs de entrada + orquestração |

---

## 8. Como Executar

### Docker (recomendado)

```bash
# Subir tudo
./run.sh

# Usar o cliente
docker attach client

# Testar falha do líder
docker stop node_X

# Testar recuperação
docker start node_X

# Limpar tudo (incluindo dados persistidos)
docker compose down --volumes
```

### Regenerar stubs após alterar o .proto

```bash
# Python
uv run python -m grpc_tools.protoc -I src/proto --python_out=src/generated --grpc_python_out=src/generated src/proto/raft.proto
sed -i 's/^import raft_pb2 as raft__pb2$/from . import raft_pb2 as raft__pb2/' src/generated/raft_pb2_grpc.py

# Go
protoc -I src/proto --go_out=client/pb --go_opt=paths=source_relative --go-grpc_out=client/pb --go-grpc_opt=paths=source_relative src/proto/raft.proto
```

---

## 9. Pontos-Chave para a Apresentação

### Cenário 1 — Operação Normal
- Subir cluster → eleição automática → publicar dados → consumir

### Cenário 2 — Falha do Líder
- `docker stop` no líder → nova eleição em 2-5s → continuar operações

### Cenário 3 — Persistência
- Parar nó → reiniciar → estado recuperado do JSON

### Cenário 4 — Recuperação de Réplica
- Parar réplica → publicar novos dados → reiniciar réplica →
  líder detecta gap via `next_index` → envia APENAS entradas faltantes

### Cenário 5 — Consistência de Leitura
- `consume` no líder e em followers → ambos retornam APENAS dados committed
- Dados uncommitted (sem quórum) NÃO aparecem

### Armadilhas comuns
- O quórum é **sempre 3 de 4**, nunca reduz com falhas
- Se 2 nós caem, escritas ficam pendentes (sem quórum)
- O cliente descobre o líder **via respostas dos nós**, não via registro externo
- O cliente Go **não pode** chamar RequestVote ou AppendEntries

---

## 10. Timeouts

| Parâmetro | Valor | Por quê |
|---|---|---|
| Election timeout | 2000-5000ms (aleatório) | Evita split vote, detecta falha do líder |
| Heartbeat interval | 500ms | Deve ser << election timeout mínimo |
| gRPC timeout | 2-3s | Detecta nó inacessível sem travar |
| Tick interval | 100ms | Frequência de verificação dos timers |
| Startup delay | 3s | Espera todos os nós iniciarem |
