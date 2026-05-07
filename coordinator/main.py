"""Coordinator: client-facing API + transaction coordinator.

Responsibilities (mirroring the textbook's "transaction coordinator"):

  1. Routing       — sha256(key) % N picks the shard for each key.
  2. Concurrency   — strict two-phase locking on every read/write.
                     Deadlocks are broken by timeout (presumed deadlock).
  3. 2PC           — /commit runs Phase 1 (/prepare to every shard),
                     persists a single decision in the WAL, then runs
                     Phase 2 (/commit or /abort to each shard).
  4. Recovery      — on startup, every recorded decision is re-broadcast
                     (idempotent), and any shard's in-doubt prepared txn
                     with no logged decision is presumed-aborted.
  5. Failover      — a heartbeat task pings each leader; after K failures
                     the follower is /promoted and the shard map updated.

Everything below is organised in roughly that order.
"""
import asyncio
import os
import threading
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Set

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common.config import get_num_shards, get_shard_map
from common.hashing import shard_for_key
from common.locks import DeadlockTimeout, LockManager
from common.wal import WAL

# ---------------------------------------------------------------- config
NUM_SHARDS = get_num_shards()
SHARDS: Dict[int, Dict[str, Any]] = get_shard_map()
CONFIGURED_REPLICAS: Dict[int, List[str]] = {
    sid: [cfg["leader"]] + list(cfg.get("followers", []))
    for sid, cfg in SHARDS.items()
}

LOCK_TIMEOUT_S = float(os.getenv("LOCK_TIMEOUT_S", "5"))
HEARTBEAT_INTERVAL_S = float(os.getenv("HEARTBEAT_INTERVAL_S", "2"))
LEADER_FAIL_THRESHOLD = int(os.getenv("LEADER_FAIL_THRESHOLD", "3"))
WAL_DIR = os.getenv("WAL_DIR", "/data")

# ---------------------------------------------------------------- state
decision_log = WAL(os.path.join(WAL_DIR, "coordinator.wal"))
locks = LockManager(timeout_s=LOCK_TIMEOUT_S)

# Active transactions:
#   txn_id -> {
#       "shards":  set[int]                — participants we've touched
#       "updates": dict[int, dict[k, v]]   — buffered writes per shard
#                                            (sent to /prepare so a
#                                            failed leader can be skipped)
#   }
transactions: Dict[str, Dict[str, Any]] = {}
_txn_mu = threading.Lock()

# Per-shard consecutive heartbeat-failure counter (used only by the
# background task; never on the request path).
_failure_count: Dict[int, int] = {sid: 0 for sid in SHARDS}
_shutdown = asyncio.Event()


# ---------------------------------------------------------------- helpers
def _leader(shard_id: int) -> str:
    return SHARDS[shard_id]["leader"]


def _followers(shard_id: int) -> List[str]:
    return list(SHARDS[shard_id].get("followers", []))


def _unique_urls(urls: List[str]) -> List[str]:
    out: List[str] = []
    for url in urls:
        if url and url not in out:
            out.append(url)
    return out


def _replica_candidates(shard_id: int) -> List[str]:
    current = [_leader(shard_id)] + _followers(shard_id)
    return _unique_urls(CONFIGURED_REPLICAS.get(shard_id, []) + current)


def _replica_priority(shard_id: int, url: str) -> int:
    candidates = _replica_candidates(shard_id)
    return candidates.index(url) if url in candidates else -1


def _set_shard_replicas(shard_id: int, leader: str, followers: List[str]) -> None:
    SHARDS[shard_id] = {
        "leader": leader,
        "followers": followers,
        "follower": followers[0] if followers else "",
    }


def _post(url: str, json: Dict[str, Any], timeout: float = 5.0) -> httpx.Response:
    return httpx.post(url, json=json, timeout=timeout)


def _read_from_node(base_url: str, shard_id: int, req: "ReadReq", read_mode: str) -> Dict[str, Any]:
    r = _post(f"{base_url}/read", req.model_dump())
    r.raise_for_status()
    out = r.json()
    out["shard_id"] = shard_id
    out["read_mode"] = read_mode
    out["routed_to"] = base_url
    return out


def _replication_lag_snapshot() -> Dict[str, Any]:
    shards: Dict[int, Any] = {}
    for sid in sorted(SHARDS):
        nodes = []
        leader_records: Optional[int] = None
        for url in _replica_candidates(sid):
            node = {
                "url": url,
                "is_current_leader": url == _leader(sid),
                "reachable": False,
                "role": None,
                "record_count": None,
                "lag_from_leader": None,
            }
            try:
                r = httpx.get(f"{url}/health", timeout=1.0)
                if r.status_code == 200:
                    h = r.json()
                    node["reachable"] = True
                    node["role"] = h.get("role")
                    node["record_count"] = h.get("record_count", h.get("committed_keys"))
                    if url == _leader(sid):
                        leader_records = node["record_count"]
            except httpx.HTTPError:
                pass
            nodes.append(node)
        if leader_records is None:
            counts = [n["record_count"] for n in nodes if n["record_count"] is not None]
            leader_records = max(counts) if counts else None
        for node in nodes:
            if leader_records is not None and node["record_count"] is not None:
                node["lag_from_leader"] = max(0, leader_records - node["record_count"])
        shards[sid] = {
            "leader": _leader(sid),
            "leader_record_count": leader_records,
            "nodes": nodes,
        }
    return {"shards": shards}


def _broadcast(endpoint: str, txn_id: str, participants: Set[int]) -> Dict[int, Any]:
    """POST {leader}/<endpoint> {txn_id} to every participant. Best-effort:
    we record per-shard outcomes but don't raise on individual failures —
    the participant will reconcile via WAL replay if it crashed."""
    out: Dict[int, Any] = {}
    for sid in participants:
        url = f"{_leader(sid)}/{endpoint}"
        try:
            r = _post(url, {"txn_id": txn_id}, timeout=10.0 if endpoint == "commit" else 5.0)
            out[sid] = {
                "ok": r.status_code == 200,
                "status": r.status_code,
                "body": r.json() if r.status_code == 200 else r.text,
            }
        except httpx.HTTPError as e:
            out[sid] = {"ok": False, "error": str(e)}
    return out


def _forget_txn(txn_id: str) -> None:
    """Drop coordinator-local state for a finished txn. Idempotent.
    Callers must have already notified the shards (via 2PC phase 2 or
    a direct /abort broadcast)."""
    with _txn_mu:
        transactions.pop(txn_id, None)
    locks.release_all(txn_id)


def _abort_txn(txn_id: str, participants: Optional[Set[int]] = None) -> None:
    """End-to-end abort: broadcast /abort to shards then forget locally.
    Used by deadlock-timeout, by /abort, and by 2PC's prepare-failed path."""
    if participants is None:
        with _txn_mu:
            st = transactions.get(txn_id)
            participants = set(st["shards"]) if st else set()
    if participants:
        _broadcast("abort", txn_id, participants)
    _forget_txn(txn_id)


def _acquire_or_abort(txn_id: str, sid: int, key: str, mode: str) -> None:
    """Acquire a lock or abort the txn end-to-end on deadlock timeout."""
    try:
        locks.acquire(txn_id, (sid, key), mode)
    except DeadlockTimeout as e:
        _abort_txn(txn_id)
        raise HTTPException(status_code=409, detail=f"deadlock-aborted: {e}")


# ---------------------------------------------------------------- 2PC
def _phase1_prepare(txn_id: str, participants: Set[int],
                    updates: Dict[int, Dict[str, str]]) -> Dict[int, Dict[str, Any]]:
    """Send /prepare to each leader and collect votes."""
    votes: Dict[int, Dict[str, Any]] = {}
    for sid in participants:
        url = f"{_leader(sid)}/prepare"
        body = {"txn_id": txn_id, "updates": updates.get(sid, {})}
        try:
            r = _post(url, body)
            ready = r.status_code == 200 and r.json().get("vote") == "ready"
            votes[sid] = (
                {"ok": True, "vote": "ready"} if ready
                else {"ok": False, "vote": "no", "status": r.status_code, "body": r.text}
            )
        except httpx.HTTPError as e:
            votes[sid] = {"ok": False, "vote": "unreachable", "error": str(e)}
    return votes


def _record_decision(txn_id: str, decision: str, participants: Set[int],
                     reason: Optional[str] = None) -> None:
    rec = {"type": "decision", "txn_id": txn_id, "decision": decision,
           "participants": sorted(participants)}
    if reason:
        rec["reason"] = reason
    decision_log.append(rec)


# ---------------------------------------------------------------- recovery
def _recover() -> None:
    """Replay coordinator decisions, then resolve shard in-doubt txns."""
    decisions: Dict[str, Dict[str, Any]] = {}
    for rec in decision_log.replay():
        if rec.get("type") == "decision":
            decisions[rec["txn_id"]] = rec  # last write wins

    # 1. Re-broadcast every decision (commit/abort is idempotent on shards).
    for tid, rec in decisions.items():
        _broadcast(rec["decision"], tid, set(rec.get("participants", [])))

    # 2. Find prepared txns we have no record of and presume-abort them.
    for sid in SHARDS:
        try:
            r = httpx.get(f"{_leader(sid)}/status", timeout=3.0)
            in_doubt = r.json().get("in_doubt_txns", [])
        except httpx.HTTPError:
            continue
        for tid in in_doubt:
            if tid in decisions:
                continue
            try:
                _post(f"{_leader(sid)}/abort", {"txn_id": tid}, timeout=3.0)
            except httpx.HTTPError:
                pass
            _record_decision(tid, "abort", {sid}, reason="presumed-abort-on-recovery")


# ---------------------------------------------------------------- heartbeat
async def _heartbeat_loop() -> None:
    """Ping every leader every HEARTBEAT_INTERVAL_S; elect a reachable
    replica after LEADER_FAIL_THRESHOLD consecutive failures."""
    async with httpx.AsyncClient(timeout=2.0) as client:
        while not _shutdown.is_set():
            for sid in list(SHARDS):
                leader_url = SHARDS[sid]["leader"]
                alive = False
                try:
                    r = await client.get(f"{leader_url}/health")
                    alive = r.status_code == 200 and r.json().get("role") == "leader"
                except httpx.HTTPError:
                    pass

                if alive:
                    _failure_count[sid] = 0
                    continue

                _failure_count[sid] += 1
                if _failure_count[sid] >= LEADER_FAIL_THRESHOLD:
                    candidates = sorted(
                        _replica_candidates(sid),
                        key=lambda url: _replica_priority(sid, url),
                        reverse=True,
                    )
                    reachable = []
                    for candidate_url in candidates:
                        try:
                            hr = await client.get(f"{candidate_url}/health")
                            if hr.status_code == 200:
                                reachable.append(candidate_url)
                        except httpx.HTTPError:
                            pass
                    if not reachable:
                        continue
                    elected = reachable[0]
                    remaining_followers = [url for url in reachable if url != elected]
                    try:
                        rp = await client.post(
                            f"{elected}/promote",
                            json={
                                "new_follower_url": remaining_followers[0] if remaining_followers else None,
                                "new_follower_urls": remaining_followers,
                            },
                        )
                        if rp.status_code == 200:
                            _set_shard_replicas(sid, elected, remaining_followers)
                            _failure_count[sid] = 0
                    except httpx.HTTPError:
                        pass
            try:
                await asyncio.wait_for(_shutdown.wait(), timeout=HEARTBEAT_INTERVAL_S)
            except asyncio.TimeoutError:
                pass


# ---------------------------------------------------------------- lifespan
@asynccontextmanager
async def lifespan(_app: FastAPI):
    await asyncio.sleep(0.2)              # let shards finish booting in compose
    try:
        await asyncio.to_thread(_recover)
    except Exception:
        pass                              # recovery is best-effort
    task = asyncio.create_task(_heartbeat_loop())
    try:
        yield
    finally:
        _shutdown.set()
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


app = FastAPI(title="Coordinator", lifespan=lifespan)


# ---------------------------------------------------------------- models
class WriteReq(BaseModel):
    txn_id: str
    key: str
    value: str


class ReadReq(BaseModel):
    txn_id: Optional[str] = None
    key: str


class TxnReq(BaseModel):
    txn_id: str


# ---------------------------------------------------------------- endpoints
@app.get("/cluster")
def cluster():
    return {
        "num_shards": NUM_SHARDS,
        "shards": SHARDS,
        "hash_scheme": "sha256(key) % num_shards",
        "active_txns": len(transactions),
        "lock_timeout_s": LOCK_TIMEOUT_S,
        "heartbeat_interval_s": HEARTBEAT_INTERVAL_S,
        "leader_fail_threshold": LEADER_FAIL_THRESHOLD,
    }


@app.get("/leader-validity")
def leader_validity(shard_id: int, node_url: str):
    if shard_id not in SHARDS:
        raise HTTPException(status_code=404, detail="unknown shard")
    current_leader = _leader(shard_id)
    return {
        "shard_id": shard_id,
        "node_url": node_url,
        "current_leader": current_leader,
        "valid": current_leader == node_url,
    }


@app.get("/replication-lag")
def replication_lag():
    return _replication_lag_snapshot()


@app.get("/locks")
def lock_state():
    return locks.snapshot()


@app.get("/transactions")
def list_transactions():
    """UI hook: every active transaction and the shards/keys it has touched."""
    with _txn_mu:
        return {
            tid: {"shards": sorted(st["shards"]), "updates": st["updates"]}
            for tid, st in transactions.items()
        }


@app.get("/decisions")
def recent_decisions(limit: int = 20):
    """Last N committed/aborted txn decisions from the WAL. UI hook."""
    records = [r for r in decision_log.replay() if r.get("type") == "decision"]
    return {"decisions": records[-limit:]}


@app.post("/begin")
def begin():
    txn_id = str(uuid.uuid4())
    with _txn_mu:
        transactions[txn_id] = {"shards": set(), "updates": {}}
    return {"txn_id": txn_id}


@app.post("/write")
def write(req: WriteReq):
    with _txn_mu:
        if req.txn_id not in transactions:
            raise HTTPException(status_code=400, detail="unknown txn_id; call /begin first")
    sid = shard_for_key(req.key, NUM_SHARDS)

    _acquire_or_abort(req.txn_id, sid, req.key, "X")

    try:
        r = _post(f"{_leader(sid)}/write", req.model_dump())
        r.raise_for_status()
    except httpx.HTTPError as e:
        # Locks stay held — the client can /abort to release them.
        raise HTTPException(status_code=502, detail=f"shard {sid} write failed: {e}")

    with _txn_mu:
        st = transactions[req.txn_id]
        st["shards"].add(sid)
        st["updates"].setdefault(sid, {})[req.key] = req.value
    return {"ok": True, "shard_id": sid, "result": r.json()}


@app.post("/read")
def read(req: ReadReq):
    sid = shard_for_key(req.key, NUM_SHARDS)
    if req.txn_id:
        with _txn_mu:
            if req.txn_id not in transactions:
                raise HTTPException(status_code=400, detail="unknown txn_id")
        _acquire_or_abort(req.txn_id, sid, req.key, "S")
        try:
            return _read_from_node(_leader(sid), sid, req, "transactional-leader")
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"shard {sid} read failed: {e}")

    errors = []
    for follower_url in _followers(sid):
        try:
            return _read_from_node(follower_url, sid, req, "eventual-follower")
        except httpx.HTTPError as e:
            errors.append(f"{follower_url}: {e}")
    try:
        return _read_from_node(_leader(sid), sid, req, "committed-leader-fallback")
    except httpx.HTTPError as e:
        errors.append(f"{_leader(sid)}: {e}")
        raise HTTPException(status_code=502, detail=f"shard {sid} read failed: {'; '.join(errors)}")


@app.post("/commit")
def commit(req: TxnReq):
    """Two-phase commit across the txn's participating shards."""
    with _txn_mu:
        if req.txn_id not in transactions:
            raise HTTPException(status_code=400, detail="unknown txn_id")
        st = transactions[req.txn_id]
        participants: Set[int] = set(st["shards"])
        per_shard_updates: Dict[int, Dict[str, str]] = {
            sid: dict(u) for sid, u in st["updates"].items()
        }

    # Empty txn: nothing to do.
    if not participants:
        _forget_txn(req.txn_id)
        return {"ok": True, "decision": "commit", "committed_shards": [],
                "votes": {}, "results": {}, "note": "empty txn"}

    # Phase 1
    votes = _phase1_prepare(req.txn_id, participants, per_shard_updates)
    all_ready = all(v.get("vote") == "ready" for v in votes.values())

    # Decision (this is the durable point of no return)
    decision = "commit" if all_ready else "abort"
    _record_decision(req.txn_id, decision, participants,
                     reason=None if all_ready else "prepare-failed")

    # Phase 2
    results = _broadcast(decision, req.txn_id, participants)

    # Free local state regardless of phase-2 outcome — the WAL decision
    # is authoritative; participants will reconcile on restart.
    _forget_txn(req.txn_id)

    if decision == "abort":
        raise HTTPException(status_code=409, detail={
            "message": "commit aborted in prepare phase",
            "votes": votes, "aborts": results,
        })

    if not all(v.get("ok") for v in results.values()):
        raise HTTPException(status_code=500, detail={
            "message": "commit decided but some participants did not ack",
            "votes": votes, "results": results,
        })

    return {
        "ok": True, "decision": "commit",
        "committed_shards": sorted(participants),
        "votes": votes, "results": results,
    }


@app.post("/abort")
def abort(req: TxnReq):
    with _txn_mu:
        if req.txn_id not in transactions:
            raise HTTPException(status_code=400, detail="unknown txn_id")
        participants: Set[int] = set(transactions[req.txn_id]["shards"])

    if participants:
        _record_decision(req.txn_id, "abort", participants, reason="client-abort")
    results = _broadcast("abort", req.txn_id, participants)
    _forget_txn(req.txn_id)
    return {"ok": True, "aborted_shards": sorted(participants), "results": results}
