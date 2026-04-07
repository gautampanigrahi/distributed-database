# Distributed Database — Sprint 1 Prototype

A minimal distributed key-value store with hash-based sharding,
coordinator-based routing, leader/follower replication, and basic
transactions. Built with FastAPI + Docker Compose.

## Architecture

```
              ┌──────────────┐
  clients ───▶│ Coordinator  │  :8000  (single client-facing API)
              └──┬────────┬──┘
                 │        │        route: shard_id = sha256(key) % N
        ┌────────▼──┐  ┌──▼────────┐
        │ shard0    │  │ shard1    │
        │  leader   │  │  leader   │  :8001 / :8003
        │     │     │  │     │     │
        │     ▼     │  │     ▼     │  sync replication on commit
        │ follower  │  │ follower  │  :8002 / :8004
        └───────────┘  └───────────┘
```

| Node              | Host port | Role     |
|-------------------|-----------|----------|
| coordinator       | 8000      | router   |
| shard0-leader     | 8001      | leader   |
| shard0-follower   | 8002      | follower |
| shard1-leader     | 8003      | leader   |
| shard1-follower   | 8004      | follower |

## What's implemented (Sprint 1)

- [x] Deterministic sha256-based sharding
- [x] Coordinator routing (`/read`, `/write`, `/begin`, `/commit`, `/abort`)
- [x] Per-txn staging (read-your-own-writes within a transaction)
- [x] Leader → follower synchronous replication with strong consistency
  (commit fails if follower does not ACK)
- [x] Single-shard ACID transactions
- [x] In-memory storage
- [x] Docker Compose cluster of 5 services

## What's deferred (future sprints — see `TODO` comments)

- [ ] Two-phase commit for multi-shard atomicity
- [ ] Strict 2PL (concurrent txn isolation / locking)
- [ ] Persistent WAL / recovery
- [ ] Leader failover, follower promotion
- [ ] Snapshot isolation across the cluster

## Running

```bash
docker compose up --build
```

Wait until all five services report healthy (`/health`), then in another
terminal:

```bash
python demo.py
```

## Manual API smoke-test

```bash
curl http://localhost:8000/cluster

TXN=$(curl -s -XPOST http://localhost:8000/begin | python -c 'import sys,json;print(json.load(sys.stdin)["txn_id"])')
curl -s -XPOST http://localhost:8000/write \
     -H 'content-type: application/json' \
     -d "{\"txn_id\":\"$TXN\",\"key\":\"user:1\",\"value\":\"alice\"}"
curl -s -XPOST http://localhost:8000/commit \
     -H 'content-type: application/json' \
     -d "{\"txn_id\":\"$TXN\"}"
curl -s -XPOST http://localhost:8000/read \
     -H 'content-type: application/json' \
     -d '{"key":"user:1"}'
```

Inspect a shard's committed + staged state directly:

```bash
curl http://localhost:8001/data   # shard0 leader
curl http://localhost:8002/data   # shard0 follower (should match after commit)
```

## Tests

```bash
pip install -r requirements.txt
pytest -v
```

Covers:
- sha256 hashing is deterministic and balanced
- shard write → read-your-own-writes → commit → committed visibility
- abort clears staged writes
- follower replicate endpoint

## Demo walkthrough

`demo.py` shows:
1. Cluster topology via `GET /cluster`
2. Begin a transaction
3. Write 5 keys — sha256 routing puts them on different shards
4. Staged reads inside the txn return the unpublished values
5. Commit — each touched leader replicates to its follower, then applies
6. Plain reads after commit return committed values
7. A second txn is started, written, then aborted — the key stays absent

## File layout

```
distributed-database/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── demo.py
├── common/
│   ├── __init__.py
│   ├── hashing.py          # sha256-based shard mapping
│   └── config.py           # env-driven shard topology
├── coordinator/
│   ├── __init__.py
│   └── main.py             # client API + routing + txn tracking
├── shard/
│   ├── __init__.py
│   └── main.py             # leader/follower storage node
└── tests/
    ├── __init__.py
    ├── test_hashing.py
    └── test_shard.py
```
