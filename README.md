# Distributed Database Final Project

A distributed, sharded key-value database implemented in Python with FastAPI and Docker Compose. The system supports hash-based sharding, multi-follower replication, transactional writes, two-phase commit, strict two-phase locking, follower reads, failover, write-ahead logging, and a live dashboard for observing the cluster.

## Team Members

- Gautam Panigrahi
- Ankush Chandrashekar

## Project Overview

The database is organized around a coordinator and multiple shard replicas. Clients send requests to the coordinator. The coordinator routes keys to shards using deterministic SHA-256 hashing, manages transaction state, applies strict two-phase locking, and runs two-phase commit for multi-shard transactions.

Each shard has one leader and multiple followers. Writes go through the shard leader and are replicated to followers. Non-transactional reads can be served by followers for better read scalability, while transactional reads continue to use the leader path to preserve lock-based consistency and read-your-own-writes behavior.

The default Docker Compose topology is:

| Component | Host Port | Role |
|---|---:|---|
| coordinator | 8000 | client API, transaction coordinator, lock manager |
| shard0-leader | 8001 | shard 0 leader |
| shard0-follower | 8002 | shard 0 follower |
| shard0-follower-2 | 8003 | shard 0 follower |
| shard1-leader | 8004 | shard 1 leader |
| shard1-follower | 8005 | shard 1 follower |
| shard1-follower-2 | 8006 | shard 1 follower |
| shard2-leader | 8007 | shard 2 leader |
| shard2-follower | 8008 | shard 2 follower |
| shard2-follower-2 | 8009 | shard 2 follower |

## Features

- Hash-based sharding using `sha256(key) % num_shards`
- Coordinator-based transaction routing
- Strict two-phase locking with shared and exclusive locks
- Timeout-based deadlock handling
- Two-phase commit across shards
- Write-ahead logging for coordinator decisions and shard state
- Leader-to-follower replication
- Configurable multiple followers per shard
- Non-transactional follower reads with leader fallback
- Transactional reads routed through leaders
- Read-your-own-writes inside transactions
- Heartbeat-based failover
- Coordinator-driven leader election among replicas
- Stale restarted leader self-demotion
- Replication lag and record-count monitoring
- Terminal dashboard
- Evaluation scripts and HTML eval report generation

## Requirements

Install these before running the project:

- Docker Desktop or Docker Engine with Docker Compose
- Python 3.11 recommended
- `pip`

The Python dependencies are listed in `requirements.txt`:

```bash
fastapi==0.115.0
uvicorn[standard]==0.30.6
httpx==0.27.2
pydantic==2.9.2
pytest==8.3.3
rich==13.9.4
```

## Compile and Setup Instructions

This is a Python project, so there is no separate compilation step. The setup step is installing dependencies and building the Docker images.

Create and activate a virtual environment:

```bash
python3.11 -m venv .venv311
source .venv311/bin/activate
pip install -r requirements.txt
```

If `python3.11` is not available, use:

```bash
python3 -m venv .venv311
source .venv311/bin/activate
pip install -r requirements.txt
```

Build the Docker services:

```bash
docker compose build
```

## Running the Distributed Database

Start the full cluster:

```bash
docker compose up --build
```

Or run it in the background:

```bash
docker compose up -d --build
```

Check the coordinator:

```bash
curl http://localhost:8000/cluster
```

Stop the cluster:

```bash
docker compose down
```

To remove persistent database state and start fresh:

```bash
docker compose down -v
```

## Running the Dashboard

The dashboard shows coordinator state, shard health, leader/follower roles, record counts, replication lag, active transactions, locks, and recent two-phase commit decisions.

Run:

```bash
.venv311/bin/python dashboard.py
```

If your virtual environment is named `.venv`, run:

```bash
.venv/bin/python dashboard.py
```

## Running Client Commands

The CLI client talks to the coordinator at `http://localhost:8000` by default.

Begin a transaction:

```bash
.venv311/bin/python client.py begin
```

Write a value inside a transaction:

```bash
.venv311/bin/python client.py write <txn_id> user:1 alice
```

Commit a transaction:

```bash
.venv311/bin/python client.py commit <txn_id>
```

Abort a transaction:

```bash
.venv311/bin/python client.py abort <txn_id>
```

Run a non-transactional read:

```bash
.venv311/bin/python client.py read user:1
```

Run a transactional read:

```bash
.venv311/bin/python client.py read-tx <txn_id> user:1
```

Inspect cluster state:

```bash
.venv311/bin/python client.py cluster
```

Inspect locks:

```bash
.venv311/bin/python client.py locks
```

Inspect recent two-phase commit decisions:

```bash
.venv311/bin/python client.py decisions --limit 20
```

## Manual API Example

Start a transaction, write a key, commit it, and read it back:

```bash
TXN=$(curl -s -X POST http://localhost:8000/begin | python3 -c 'import json,sys; print(json.load(sys.stdin)["txn_id"])')

curl -s -X POST http://localhost:8000/write \
  -H 'content-type: application/json' \
  -d "{\"txn_id\":\"$TXN\",\"key\":\"user:1\",\"value\":\"alice\"}"

curl -s -X POST http://localhost:8000/commit \
  -H 'content-type: application/json' \
  -d "{\"txn_id\":\"$TXN\"}"

curl -s -X POST http://localhost:8000/read \
  -H 'content-type: application/json' \
  -d '{"key":"user:1"}'
```

## Demonstrating Key Behaviors

### Follower Reads

Non-transactional reads are optimized to use followers when available:

```bash
.venv311/bin/python client.py read user:1
```

The response includes fields such as:

```json
{
  "read_mode": "eventual-follower",
  "routed_to": "http://shard0-follower:8000"
}
```

If followers are unavailable, the coordinator falls back to the leader.

### Transactional Reads

Transactional reads use the leader and acquire shared locks:

```bash
.venv311/bin/python client.py read-tx <txn_id> user:1
```

The response includes:

```json
{
  "read_mode": "transactional-leader"
}
```

### Two-Phase Commit

A transaction that writes keys across multiple shards is committed using two-phase commit. The coordinator first sends `prepare` requests, records the final decision in its WAL, and then sends `commit` or `abort` to all participant shards.

### Locking

Reads inside transactions acquire shared locks. Writes acquire exclusive locks. Conflicting transactions wait until the lock is available or abort after the configured timeout.

### Failover

To demonstrate leader failover:

```bash
docker compose stop shard1-leader
```

After the heartbeat threshold is reached, the coordinator elects a reachable replica and promotes it. Check the new leader with:

```bash
curl http://localhost:8000/cluster
```

Restart the old leader:

```bash
docker compose start shard1-leader
```

The restarted node checks with the coordinator and demotes itself if it is no longer the valid leader.

## Replication Lag Monitoring

The coordinator exposes replication lag and record counts:

```bash
curl -s http://localhost:8000/replication-lag | python3 -m json.tool
```

Each node reports:

- `record_count`
- `lag_from_leader`
- `role`
- `reachable`
- `is_current_leader`

The dashboard also displays `records` and `lag` for each shard node.

## Running Tests

Run the test suite:

```bash
.venv311/bin/python -m pytest -q
```

The tests cover:

- deterministic hashing
- write-ahead logging
- shared and exclusive lock behavior
- timeout-based deadlock handling
- shard write/read/prepare/commit/abort behavior
- replication
- promotion
- coordinator two-phase commit behavior
- abort handling
- transactional and non-transactional read routing

## Running Performance Evals

Run all evals:

```bash
PYTHON_BIN=.venv311/bin/python ./run_evals.sh
```

The script writes JSON results to:

```text
eval-results/<timestamp>/
```

It also generates an HTML report:

```text
eval-results/<timestamp>/report.html
```

Open the report:

```bash
open eval-results/<timestamp>/report.html
```

Run a single baseline eval:

```bash
.venv311/bin/python evals.py baseline --workload reads --clients 16 --requests 500
```

Run a contention eval:

```bash
.venv311/bin/python evals.py contention --clients 8 --requests 50 --hot-key employee:hot
```

Generate a report manually:

```bash
.venv311/bin/python eval_report.py eval-results/<timestamp>
```

## Important Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Defines the coordinator, shard leaders, and shard followers |
| `coordinator/main.py` | Coordinator API, routing, locking, 2PC, failover, leader validity, replication lag |
| `shard/main.py` | Shard participant API, WAL recovery, replication, promotion, self-demotion |
| `common/hashing.py` | Deterministic shard hashing |
| `common/config.py` | Environment-driven shard topology configuration |
| `common/locks.py` | Strict two-phase lock manager |
| `common/wal.py` | Append-only JSON-lines write-ahead log |
| `client.py` | Command-line client |
| `dashboard.py` | Live terminal dashboard |
| `evals.py` | Performance and contention eval workloads |
| `eval_report.py` | HTML report generator for eval results |
| `run_evals.sh` | Shell script to run all evals |
| `tests/` | Unit and integration-style tests |

## External Libraries and Resources

This project uses standard open-source Python libraries for web services, testing, and terminal visualization:

- FastAPI
- Uvicorn
- httpx
- Pydantic
- pytest
- Rich

These libraries are used as infrastructure dependencies. The distributed database logic, transaction coordination, locking, replication, failover, dashboard integration, and eval scripts were implemented as project work.

## Group Member Contributions

### Gautam Panigrahi

Gautam worked on the core distributed transaction design and implementation. His responsibilities included the coordinator-side transaction lifecycle, two-phase commit orchestration, participant tracking, write-ahead decision logging, recovery behavior, and integration of strict two-phase locking with the transaction API. He also contributed to the testing strategy for coordinator behavior, commit/abort correctness, deadlock timeout handling, and multi-shard transaction validation.

### Ankush Chandrashekar

Ankush worked on the replication, availability, observability, and evaluation portions of the system. His responsibilities included multi-shard and multi-follower Docker topology, follower replication support, leader failover, leader validity checks, stale leader self-demotion, follower-read routing, replication lag monitoring, dashboard updates, command-line workflows, performance eval scripts, and the HTML eval report generator. He also contributed to validation, demo preparation, and operational scripts for running and testing the system.

## Notes for Grading

To run the complete system from a clean checkout:

```bash
python3.11 -m venv .venv311
source .venv311/bin/activate
pip install -r requirements.txt
docker compose up -d --build
.venv311/bin/python -m pytest -q
.venv311/bin/python dashboard.py
```

In another terminal, use `client.py` or `run_evals.sh` to exercise the system.
