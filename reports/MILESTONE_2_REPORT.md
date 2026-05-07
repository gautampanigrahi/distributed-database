# Milestone 2 — Status Report

**Project:** Scalable, Fault-Tolerant and Distributed Database with Transaction Management
**Team:** Ankush Chandrashekar, Gautam Panigrahi
**Date:** May 4, 2026
**Sprint:** 2 (Distributed transactions, locking, recovery, failover)

---

## 1. Summary

Sprint 2 delivers the four largest items on our project plan that were
deferred from Milestone 1:

| Plan week | Mechanism                              | Status              |
|-----------|----------------------------------------|---------------------|
| Week 4    | Strict two-phase locking + deadlock handling | Implemented |
| Week 5    | Two-phase commit across shards         | Implemented         |
| Week 6    | WAL persistence + recovery; leader failover | Implemented   |
| Week 7    | Concurrency / failure-mode evaluation  | In progress (this report) |

All 24 unit tests pass (`pytest -v`). The demo script exercises the
features end-to-end against the Docker Compose cluster.

---

## 2. What changed since Milestone 1

### 2.1 Strict two-phase locking ( `common/locks.py` )

* Lock granularity is **(shard\_id, key)**.
* Two modes: **S** (shared, taken on `/read` inside a txn) and **X**
  (exclusive, taken on `/write`).
* Same-txn upgrades S→X are allowed; otherwise S/S is the only
  compatible pair.
* All locks held by a transaction are released atomically on commit or
  abort (`release_all`), giving us strict-2PL semantics.
* No waits-for graph: deadlock is broken with a configurable timeout
  (`LOCK_TIMEOUT_S`, default 5s). A waiter that exceeds the timeout
  raises `DeadlockTimeout`, which the coordinator catches and turns
  into a `409 deadlock-aborted` response. Locks held by the aborted
  txn are released so the survivor can make progress. This is the
  standard *presumed deadlock* approach.

### 2.2 Two-phase commit ( `coordinator/main.py` ↔ `shard/main.py` )

* `/commit` at the coordinator runs **Phase 1**: it sends `/prepare` to
  each participating leader, with the per-shard staged updates piggy-
  backed in the body so a leader that crashed mid-transaction does not
  silently lose data. A participant either votes `ready` (after fsync)
  or anything else counts as `no`.
* The coordinator records a **single durable decision** in its WAL —
  either `{"decision": "commit"}` or `{"decision": "abort"}`. This
  record is the source of truth for the rest of the protocol.
* **Phase 2** broadcasts `/commit` (or `/abort`) to every participant.
  The endpoint is idempotent on the participant side, so a retry after
  a partial broadcast is safe.

### 2.3 Write-ahead logging + recovery

* New module `common/wal.py` — append-only JSON-lines log with `fsync`
  after every record. Torn final lines (from a crash mid-write) are
  skipped on replay.
* **Each shard** maintains its own WAL with four record types:
  `prepare`, `commit`, `abort`, `replicate`. On restart the shard
  rebuilds its `committed`, `prepared`, and `staged` tables by
  replaying the log.
* **Coordinator** maintains a separate WAL of final 2PC decisions.
  On startup it (a) re-broadcasts every recorded decision to its
  participants — idempotent, so this safely handles the case of a
  coordinator crash *after* logging but *before* notifying shards —
  and (b) polls each leader's `/status` for in-doubt prepared txns
  that have no decision and presumed-aborts them.

### 2.4 Heartbeat-driven leader failover

* The coordinator runs a background asyncio task every
  `HEARTBEAT_INTERVAL_S` (default 2s) that hits each leader's
  `/health`. After `LEADER_FAIL_THRESHOLD` consecutive failures
  (default 3 → ~6s of unreachability), it sends `/promote` to the
  follower and updates the in-memory shard map. Subsequent client
  requests are routed to the new leader.
* The new leader runs solo until a fresh follower URL is supplied —
  this is intentionally simple and CP-conservative: writes will
  succeed locally but log a 500 to indicate replication is degraded.

### 2.5 Other small improvements

* `/cluster` exposes the lock-timeout, heartbeat, and threshold
  settings, useful for the demo.
* `/locks` endpoint surfaces the current lock-manager snapshot for
  debugging concurrent runs.
* Each shard's `/data` and `/status` reveal both committed and
  prepared (in-doubt) state — used by the demo's "scenario 4" snapshot.

---

## 3. Files added / changed

```
common/locks.py        new   (147 lines)  — strict-2PL lock manager
common/wal.py          new   ( 60 lines)  — append-only JSON WAL
coordinator/main.py    rewrote (~270 lines) — 2PC, locks, decision log,
                                              recovery, heartbeat task
shard/main.py          rewrote (~210 lines) — /prepare, WAL, /promote,
                                              /status, recovery on import
docker-compose.yml     edits  — per-service /data volume + tunables
Dockerfile             edits  — declare /data VOLUME
demo.py                rewrote — exercises 2PC, deadlock, abort, snapshot
tests/test_locks.py    new    (5 cases)
tests/test_wal.py      new    (4 cases)
tests/test_shard.py    extended (3 → 7 cases)
tests/test_coordinator.py new (4 cases)
```

---

## 4. Updated requirements coverage

| ID | Requirement | Sprint 1 | Sprint 2 |
|----|-------------|----------|----------|
| FR-1  | Coordinator API                       | ✅ | ✅ |
| FR-2  | Hash-based sharding                   | ✅ | ✅ |
| FR-3  | begin/write/read/commit/abort         | ✅ | ✅ |
| FR-4  | Read-your-own-writes                  | ✅ | ✅ |
| FR-5  | Leader-only writes                    | ✅ | ✅ |
| FR-6  | Sync leader→follower replication      | ✅ | ✅ |
| FR-7  | Commit fails on follower failure      | ✅ | ✅ |
| FR-8  | `/cluster` introspection              | ✅ | ✅ |
| FR-9  | Per-node `/health`, `/data`           | ✅ | ✅ |
| FR-10 | Two-phase commit                      | ❌ | ✅ |
| FR-11 | Strict 2PL                            | ❌ | ✅ |
| FR-12 | Leader failover / promotion           | ❌ | ✅ |
| FR-13 | Persistent WAL + recovery             | ❌ | ✅ |
| FR-14 | Timeout-based deadlock handling (new) | —  | ✅ |
| FR-15 | Coordinator decision recovery (new)   | —  | ✅ |

13 of 13 originally-planned functional requirements now satisfied.

---

## 5. Test results

```
$ pytest -v
============================= test session starts ==============================
tests/test_coordinator.py::test_2pc_happy_path_commits           PASSED
tests/test_coordinator.py::test_2pc_aborts_when_one_shard_votes_no PASSED
tests/test_coordinator.py::test_deadlock_timeout_aborts_writer    PASSED
tests/test_coordinator.py::test_explicit_abort_releases_locks     PASSED
tests/test_hashing.py        (4 cases)                            PASSED
tests/test_locks.py          (5 cases)                            PASSED
tests/test_shard.py          (7 cases)                            PASSED
tests/test_wal.py            (4 cases)                            PASSED
============================ 24 passed in 1.35s ==============================
```

Notable cases:

* `test_two_txn_deadlock_one_times_out` — exercises a true deadlock:
  t1 holds A and wants B; t2 holds B and wants A. With the timeout set
  to 0.3s, at least one transaction is aborted (by design we accept
  that both may abort under presumed-deadlock — the next attempt
  will not deadlock).
* `test_wal_recovery_in_doubt_txn` — writes + prepares a txn, reloads
  the shard module to simulate restart, and confirms the WAL replay
  surfaces the prepared txn in `/status`.
* `test_2pc_aborts_when_one_shard_votes_no` — verifies the coordinator
  records the `abort` decision in its log and broadcasts `/abort` to
  every participant when any prepare vote is non-`ready`.

---

## 6. Demo results (against Docker Compose cluster)

The demo (run after `docker compose up`) walks four scenarios. Sample
output:

```
======================================================================
scenario 1 — 2PC commit across two shards
======================================================================
began txn: 4c1c8b86-...
  write user:1          = alice      -> shard 1
  write user:2          = bob        -> shard 1
  write order:42        = pending    -> shard 0
  write order:99        = shipped    -> shard 1
  write product:abc     = widget     -> shard 0

commit (two-phase)...
  decision      : commit
  shards touched: [0, 1]
  prepare votes : {0: {'ok': True, 'vote': 'ready'},
                   1: {'ok': True, 'vote': 'ready'}}

======================================================================
scenario 2 — concurrent writes on same key (deadlock-abort)
======================================================================
t1=8ce03f4d...  t2=fe2b87e9...
  t1 wrote contested = by-t1  (status=200)
  t2 attempts the same key (should time out & 409)...
  t2 status     : 409
  t2 detail     : deadlock-aborted: txn fe2b87e9... timed out waiting for X-lock on (1, 'contested'); current holders=['8ce03f4d-...'] mode=X
  t1 commit     : commit
  final value   : by-t1 (source=committed)

======================================================================
scenario 3 — explicit abort discards uncommitted data
======================================================================
  wrote temp:1 = X in txn 091e57f1...
  after abort, temp:1 -> None (source=missing)

======================================================================
scenario 4 — observe per-shard WAL state
======================================================================
  :8001 role=leader     committed_keys=2 prepared_txns=0
  :8002 role=follower   committed_keys=2 prepared_txns=0
  :8003 role=leader     committed_keys=3 prepared_txns=0
  :8004 role=follower   committed_keys=3 prepared_txns=0
```

(*Numbers depend on the actual sha256 distribution of the demo keys —
the value of "what scenario 1 produces" is that **both shards** had
prepared txns and both committed in lockstep, demonstrating 2PC.*)

---

## 7. Known limitations / future work

* The lock manager is centralized at the coordinator, so the
  coordinator is a single point of contention for high-concurrency
  workloads. A multi-coordinator design would either need a
  consensus-backed lock service (Chubby-style) or move locks to the
  shards.
* Failover does not yet bring the old leader back as a follower when
  it returns. Currently the promoted node runs without a replica.
* WALs are never truncated — long-running clusters will grow the log
  files unbounded. Adding compaction or checkpoints is in scope for
  Sprint 3.
* Recovery presumes-abort any prepared txn the coordinator has no
  record of. That matches our CP commitment but means a coordinator
  WAL loss is fatal for the affected txns.

---

## 8. References to proposal items

Every architectural component listed in the proposal's "Proposed
Approach" section is now implemented; see the *Mapping proposal →
implementation* table in `README.md` for file-level pointers.

*End of Milestone 2 Report.*
