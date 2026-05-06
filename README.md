# Distributed Database — Sprints 1 & 2

A scalable, fault-tolerant key–value store built around the architectural
checklist in our project proposal:

* hash-based **sharding** with a coordinator gateway
* synchronous **leader → follower replication**
* **strict two-phase locking** with timeout-based deadlock handling
* **two-phase commit** for atomic multi-shard transactions
* **write-ahead logging** on every shard and on the coordinator, with
  on-restart **recovery** for both participant in-doubt txns and
  coordinator decisions
* heartbeat-driven **leader failover** to a follower replica
* **CP** semantics: when a replica cannot be reached, writes fail
  rather than diverge

Built with FastAPI + httpx + Docker Compose.

## Architecture

```
                  ┌────────────────────────────┐
   clients  ─────▶│         Coordinator        │  :8000
                  │                            │
                  │  • routes by sha256(key)%N │
                  │  • strict 2PL lock manager │
                  │  • 2PC orchestrator        │
                  │  • decision WAL + recovery │
                  │  • heartbeat → failover    │
                  └──┬───────────┬──────────────┘
                     │           │
            shard 0  │           │  shard 1
            ┌────────▼─────┐  ┌──▼───────────┐
            │  leader      │  │  leader      │  :8001 / :8003
            │   wal        │  │   wal        │
            │     │        │  │     │        │
            │     ▼  /repl │  │     ▼  /repl │  sync replication
            │  follower    │  │  follower    │  :8002 / :8004
            │   wal        │  │   wal        │
            └──────────────┘  └──────────────┘
```

| Node              | Host port | Role     |
|-------------------|-----------|----------|
| coordinator       | 8000      | router + tx coordinator |
| shard0-leader     | 8001      | leader   |
| shard0-follower   | 8002      | follower |
| shard1-leader     | 8003      | leader   |
| shard1-follower   | 8004      | follower |

## Mapping proposal → implementation

| Proposal mechanism                                | File                       |
|---------------------------------------------------|----------------------------|
| Sharding (hash-based)                             | `common/hashing.py`        |
| Query routing                                     | `coordinator/main.py`      |
| Synchronous primary–follower replication          | `shard/main.py /commit`    |
| Leader management / failover                      | `coordinator/main.py _heartbeat_loop` + `shard/main.py /promote` |
| Strict two-phase locking                          | `common/locks.py`          |
| Timeout-based deadlock handling                   | `common/locks.py DeadlockTimeout` |
| Transaction lifecycle                             | `coordinator/main.py /begin /write /read /commit /abort` |
| Two-phase commit                                  | `coordinator/main.py /commit` + `shard/main.py /prepare /commit /abort` |
| Write propagation pipeline                        | `shard/main.py /replicate` |
| Coordinator decision recovery                     | `coordinator/main.py _recover()` + `coordinator.wal` |
| Participant prepared-state recovery               | `shard/main.py _recover_from_wal()` + per-node WAL |
| Node health monitoring                            | `coordinator/main.py _heartbeat_loop` |
| CP behavior (block/fail rather than diverge)      | replication is synchronous; commit fails if follower unreachable |

## Running

```bash
docker compose up --build
```

Wait until all five services report healthy on `/health`. Then in a
second terminal launch the **live dashboard** (the focal point of the
demo):

```bash
.venv/bin/python dashboard.py
```

It refreshes every 500 ms and shows:

* the coordinator's config + active-txn counter
* one panel per node — role, committed keys, prepared (in-doubt) txns
* every active transaction (id, shards touched, buffered writes)
* every held lock (S/X mode and which txn holds it) plus the
  deadlock-timeout counter
* the most recent 2PC decisions from the coordinator's WAL

Colour code: **green** = healthy leader, **cyan** = healthy follower,
**yellow** = leader with in-doubt prepared txns, **red** = unreachable.

In a third terminal exercise the cluster:

```bash
.venv/bin/python demo.py
```

`demo.py` walks four scenarios:

1. A 2-shard, 5-key transaction via **2PC** (prepare → commit phase)
2. Two transactions racing on the same key — the second is **deadlock-aborted** by the lock manager's timeout
3. An explicit abort that discards uncommitted writes
4. A snapshot of each shard's per-node state (committed keys, prepared txns)

## Demo flow (5 minutes)

| t   | What you do                                       | What the dashboard shows                                                |
|-----|---------------------------------------------------|-------------------------------------------------------------------------|
| 0:00| `docker compose up` + `python dashboard.py`       | All 5 nodes go green                                                    |
| 0:30| `python demo.py` scenario 1 (2PC commit)          | Both shards' `committed` counter ticks up together; a `commit` row appears in *recent 2PC decisions* |
| 1:30| Two manual transactions racing on one key         | Locks panel shows the X-holder; conflicting txn 409s with `deadlock-aborted`; deadlock-timeout counter increments |
| 2:30| `docker compose stop shard1-leader`               | shard1-leader panel turns **red**. ~6s later, shard1-follower's role flips to `leader` and its border turns **green** |
| 3:30| Run another commit hitting shard 1                | Succeeds against the promoted node                                      |
| 4:00| `docker compose start shard1-leader`              | Comes back, replays WAL, `committed` matches the rest                   |

## Manual API smoke-test

```bash
curl http://localhost:8000/cluster
curl http://localhost:8000/locks      # current lock manager state

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

Inspect a shard's WAL-derived state:

```bash
curl http://localhost:8001/data       # shard 0 leader
curl http://localhost:8001/status     # any in-doubt prepared txns
```

## Observing recovery

```bash
# 1. Run the demo to populate state.
python demo.py

# 2. Restart any one shard:
docker compose restart shard1-leader

# 3. Confirm its committed state survived (WAL replayed):
curl http://localhost:8003/data
```

To exercise leader failover:

```bash
docker compose stop shard1-leader
# Wait LEADER_FAIL_THRESHOLD * HEARTBEAT_INTERVAL_S (~6s by default).
curl http://localhost:8000/cluster   # SHARDS[1].leader is now the old follower URL.
```

## Tests

```bash
.venv/bin/python -m pytest -v
```

24 tests covering: hashing, WAL, lock manager (S/X compatibility,
timeout-based deadlock, upgrade), shard endpoints (write/read/commit,
2PC prepare path, abort, replicate, promote, WAL recovery), and
coordinator endpoints (2PC happy path, abort path, deadlock-abort,
explicit abort lock release).

## File layout

```
distributed-database/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── MILESTONE_1_REPORT.md
├── MILESTONE_2_REPORT.md
├── demo.py                # scripted scenarios for the live demo
├── dashboard.py           # rich.live TUI — operator-style monitor
├── common/
│   ├── hashing.py        # sha256-based shard mapping
│   ├── config.py         # env-driven topology
│   ├── locks.py          # strict 2PL + timeout deadlock
│   └── wal.py            # append-only JSON-lines WAL
├── coordinator/
│   └── main.py           # client API, 2PC, locks, recovery, heartbeat
├── shard/
│   └── main.py           # leader/follower with WAL + 2PC participant API
└── tests/
    ├── test_hashing.py
    ├── test_wal.py
    ├── test_locks.py
    ├── test_shard.py
    └── test_coordinator.py
```
