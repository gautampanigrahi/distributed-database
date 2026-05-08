import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common.wal import WAL

# ---------------------------------------------------------------- config
ROLE = os.getenv("ROLE", "leader")           # initial role only; /promote can change it
SHARD_ID = int(os.getenv("SHARD_ID", "0"))
NODE_ID = os.getenv("NODE_ID", f"shard{SHARD_ID}-{ROLE}")
FOLLOWER_URL = os.getenv("FOLLOWER_URL", "")
FOLLOWER_URLS = os.getenv("FOLLOWER_URLS", FOLLOWER_URL)
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "")
SELF_URL = os.getenv("SELF_URL", "")
LEADER_VALIDITY_INTERVAL_S = float(os.getenv("LEADER_VALIDITY_INTERVAL_S", "2"))
WAL_DIR = os.getenv("WAL_DIR", "/data")
WAL_PATH = os.path.join(WAL_DIR, f"{NODE_ID}.wal")

# ---------------------------------------------------------------- state
_mu = threading.Lock()                       # guards every dict below

committed: Dict[str, str] = {}               # key -> committed value
staged: Dict[str, Dict[str, str]] = {}       # txn -> {key: value}, before /prepare
prepared: Dict[str, Dict[str, str]] = {}     # txn -> {key: value}, after /prepare (in-doubt)

_role: str = ROLE                            # mutable: /promote can change to "leader"
_follower_urls: List[str] = [url.strip() for url in FOLLOWER_URLS.split(",") if url.strip()]
_shutdown = threading.Event()

wal = WAL(WAL_PATH)


# ---------------------------------------------------------------- recovery
def _recover_from_wal() -> None:
    """Rebuild in-memory state from the on-disk log on process start.

    Records understood:
        prepare    {txn_id, updates}   → prepared[txn] = updates
        commit     {txn_id}            → apply prepared[txn] to committed
        abort      {txn_id}            → drop from prepared / staged
        replicate  {updates}           → applied directly (followers)
    """
    for rec in wal.replay():
        kind = rec.get("type")
        tid = rec.get("txn_id")
        if kind == "prepare":
            prepared[tid] = dict(rec.get("updates") or {})
        elif kind == "commit" and tid in prepared:
            committed.update(prepared.pop(tid))
        elif kind == "abort":
            prepared.pop(tid, None)
        elif kind == "replicate":
            committed.update(rec.get("updates") or {})


_recover_from_wal()


# ---------------------------------------------------------------- helpers
def _require_leader() -> None:
    if _role != "leader":
        raise HTTPException(status_code=403, detail=f"node is {_role}, not leader")


def _demote_if_stale() -> bool:
    global _role, _follower_urls
    if not COORDINATOR_URL or not SELF_URL:
        return False
    with _mu:
        if _role != "leader":
            return False
    try:
        r = httpx.get(
            f"{COORDINATOR_URL.rstrip('/')}/leader-validity",
            params={"shard_id": SHARD_ID, "node_url": SELF_URL},
            timeout=2.0,
        )
        if r.status_code != 200:
            return False
        valid = bool(r.json().get("valid"))
    except httpx.HTTPError:
        return False
    if valid:
        return False
    with _mu:
        if _role == "leader":
            _role = "follower"
            _follower_urls = []
            return True
    return False


def _leader_validity_loop() -> None:
    while not _shutdown.is_set():
        _demote_if_stale()
        _shutdown.wait(LEADER_VALIDITY_INTERVAL_S)


def _do_prepare(txn_id: str, updates: Dict[str, str]) -> None:
    """Persist a prepare record and move the txn into 'prepared' state.
    Caller must hold _mu."""
    wal.append({"type": "prepare", "txn_id": txn_id, "updates": dict(updates)})
    prepared[txn_id] = dict(updates)
    staged.pop(txn_id, None)


def _replicate_to_followers(txn_id: str, updates: Dict[str, str]) -> None:
    """Push committed updates to followers. Raises HTTPException if it
    cannot be reached / refuses; caller decides whether to apply locally."""
    if not _follower_urls:
        return
    failures = []
    for follower_url in _follower_urls:
        try:
            r = httpx.post(
                f"{follower_url}/replicate",
                json={"updates": updates, "txn_id": txn_id},
                timeout=5.0,
            )
            if r.status_code != 200:
                failures.append(f"{follower_url}: {r.status_code} {r.text}")
        except httpx.RequestError as e:
            failures.append(f"{follower_url}: unreachable: {e}")
    if failures:
        raise HTTPException(status_code=500, detail={"replication_failures": failures})


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


class PrepareReq(BaseModel):
    txn_id: str
    # Coordinator may ship updates explicitly so a staged-update loss
    # (e.g. shard restart between /write and /prepare) is recoverable.
    updates: Optional[Dict[str, str]] = None


class ReplicateReq(BaseModel):
    updates: Dict[str, str]
    txn_id: Optional[str] = None             # informational, for the WAL


class PromoteReq(BaseModel):
    new_follower_url: Optional[str] = None
    new_follower_urls: Optional[List[str]] = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _demote_if_stale()
    t = threading.Thread(target=_leader_validity_loop, daemon=True)
    t.start()
    try:
        yield
    finally:
        _shutdown.set()
        t.join(timeout=1.0)


# ---------------------------------------------------------------- app
app = FastAPI(title=f"Shard {SHARD_ID} ({ROLE})", lifespan=lifespan)


# ---- introspection ----------------------------------------------------
@app.get("/health")
def health():
    with _mu:
        return {
            "node": NODE_ID,
            "role": _role,
            "shard_id": SHARD_ID,
            "ok": True,
            "committed_keys": len(committed),
            "record_count": len(committed),
            "open_txns": len(staged),
            "prepared_txns": len(prepared),
            "follower_url": _follower_urls[0] if _follower_urls else None,
            "follower_urls": list(_follower_urls),
            "coordinator_url": COORDINATOR_URL or None,
            "self_url": SELF_URL or None,
        }


@app.get("/data")
def data():
    with _mu:
        return {
            "node": NODE_ID,
            "role": _role,
            "committed": dict(committed),
            "staged": {k: dict(v) for k, v in staged.items()},
            "prepared": {k: dict(v) for k, v in prepared.items()},
        }


@app.get("/status")
def status():
    """Used by the coordinator on its own recovery to find in-doubt txns."""
    with _mu:
        return {
            "node": NODE_ID,
            "role": _role,
            "in_doubt_txns": sorted(prepared.keys()),
        }


# ---- transaction API --------------------------------------------------
@app.post("/write")
def write(req: WriteReq):
    """Stage a write. Nothing is logged yet — only /prepare flushes to disk."""
    _require_leader()
    with _mu:
        staged.setdefault(req.txn_id, {})[req.key] = req.value
        return {"ok": True, "node": NODE_ID, "staged": dict(staged[req.txn_id])}


@app.post("/read")
def read(req: ReadReq):
    """Read-your-own-writes inside a txn; otherwise return committed value."""
    with _mu:
        if req.txn_id and req.key in staged.get(req.txn_id, {}):
            return {"key": req.key, "value": staged[req.txn_id][req.key],
                    "source": "staged", "node": NODE_ID}
        if req.key in committed:
            return {"key": req.key, "value": committed[req.key],
                    "source": "committed", "node": NODE_ID}
        return {"key": req.key, "value": None, "source": "missing", "node": NODE_ID}


@app.post("/prepare")
def prepare(req: PrepareReq):
    """Phase 1 of 2PC. fsyncs the prepare record before voting ready —
    after this returns 'ready', this shard *promises* it can commit if
    asked, even after a crash and restart."""
    _require_leader()
    with _mu:
        updates = req.updates if req.updates is not None else staged.get(req.txn_id, {})
        _do_prepare(req.txn_id, updates)
        return {"vote": "ready", "node": NODE_ID, "applied": len(updates)}


@app.post("/commit")
def commit(req: TxnReq):
    """Phase 2 commit. Idempotent.

    Order matters:
      1. write the WAL decision  ── so a crash here is recoverable
      2. replicate to follower   ── so follower never falls behind
      3. apply in memory         ── only after both succeed

    If /prepare was never called we auto-prepare any staged data to keep
    single-shard direct usage simple.
    """
    _require_leader()
    with _mu:
        if req.txn_id not in prepared:
            updates = staged.pop(req.txn_id, {})
            if not updates:
                # No-op commit; still log it so replay is deterministic.
                wal.append({"type": "commit", "txn_id": req.txn_id})
                return {"ok": True, "applied": 0, "node": NODE_ID,
                        "note": "no staged writes"}
            _do_prepare(req.txn_id, updates)

        updates = prepared[req.txn_id]
        wal.append({"type": "commit", "txn_id": req.txn_id})

        try:
            _replicate_to_followers(req.txn_id, updates)
        except HTTPException:
            # Decision is already durable; apply locally so we don't
            # diverge from the WAL, then surface the replication error.
            committed.update(updates)
            prepared.pop(req.txn_id, None)
            raise

        committed.update(updates)
        prepared.pop(req.txn_id, None)
        return {"ok": True, "applied": len(updates), "node": NODE_ID}


@app.post("/abort")
def abort(req: TxnReq):
    """Phase-2 abort, or unilateral abort of a not-yet-prepared txn."""
    _require_leader()
    with _mu:
        wal.append({"type": "abort", "txn_id": req.txn_id})
        staged.pop(req.txn_id, None)
        prepared.pop(req.txn_id, None)
        return {"ok": True, "node": NODE_ID}


# ---- replication ------------------------------------------------------
@app.post("/replicate")
def replicate(req: ReplicateReq):
    """Leader pushes committed updates to its follower. Logged before
    applying so a follower crash mid-replicate doesn't lose data."""
    with _mu:
        wal.append({"type": "replicate", "updates": dict(req.updates),
                    "txn_id": req.txn_id})
        committed.update(req.updates)
        return {"ok": True, "applied": len(req.updates), "node": NODE_ID}


# ---- failover ---------------------------------------------------------
@app.post("/promote")
def promote(req: PromoteReq):
    """Convert this node from follower to leader. Called by the
    coordinator's failover routine. After promotion the new leader runs
    solo until a fresh follower URL is supplied."""
    global _role, _follower_urls
    with _mu:
        _role = "leader"
        if req.new_follower_urls is not None:
            _follower_urls = list(req.new_follower_urls)
        else:
            _follower_urls = [req.new_follower_url] if req.new_follower_url else []
        return {"ok": True, "promoted": NODE_ID,
                "follower_url": _follower_urls[0] if _follower_urls else None,
                "follower_urls": list(_follower_urls)}
