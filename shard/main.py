"""Shard node: behaves as leader or follower based on ROLE env var.

Storage: in-memory dict for committed state + per-txn staging area.
Leader:    accepts writes, stages per-txn, replicates to follower on commit.
Follower:  applies updates from leader's /replicate; rejects direct writes.

TODO(sprint2): persistent write-ahead log, strict 2PL for isolation,
               leader failover / follower promotion.
"""
import os
from typing import Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

ROLE = os.getenv("ROLE", "leader")
SHARD_ID = int(os.getenv("SHARD_ID", "0"))
NODE_ID = os.getenv("NODE_ID", f"shard{SHARD_ID}-{ROLE}")
FOLLOWER_URL = os.getenv("FOLLOWER_URL", "")

app = FastAPI(title=f"Shard {SHARD_ID} ({ROLE})")

# ------------------------------------------------------------------ state
committed: Dict[str, str] = {}                    # key -> value
staged: Dict[str, Dict[str, str]] = {}            # txn_id -> {key: value}


# ------------------------------------------------------------------ models
class WriteReq(BaseModel):
    txn_id: str
    key: str
    value: str


class ReadReq(BaseModel):
    txn_id: Optional[str] = None
    key: str


class TxnReq(BaseModel):
    txn_id: str


class ReplicateReq(BaseModel):
    updates: Dict[str, str]


# ------------------------------------------------------------------ endpoints
@app.get("/health")
def health():
    return {
        "node": NODE_ID,
        "role": ROLE,
        "shard_id": SHARD_ID,
        "ok": True,
        "committed_keys": len(committed),
        "open_txns": len(staged),
    }


@app.get("/data")
def data():
    return {"node": NODE_ID, "committed": committed, "staged": staged}


@app.post("/write")
def write(req: WriteReq):
    if ROLE != "leader":
        raise HTTPException(status_code=403, detail="writes only accepted by leader")
    staged.setdefault(req.txn_id, {})[req.key] = req.value
    return {"ok": True, "node": NODE_ID, "staged": staged[req.txn_id]}


@app.post("/read")
def read(req: ReadReq):
    # Txn-local staged value takes precedence (read-your-own-writes).
    if req.txn_id and req.txn_id in staged and req.key in staged[req.txn_id]:
        return {
            "key": req.key,
            "value": staged[req.txn_id][req.key],
            "source": "staged",
            "node": NODE_ID,
        }
    if req.key in committed:
        return {
            "key": req.key,
            "value": committed[req.key],
            "source": "committed",
            "node": NODE_ID,
        }
    return {"key": req.key, "value": None, "source": "missing", "node": NODE_ID}


@app.post("/commit")
def commit(req: TxnReq):
    if ROLE != "leader":
        raise HTTPException(status_code=403, detail="commit only on leader")
    updates = staged.pop(req.txn_id, None)
    if updates is None:
        return {"ok": True, "applied": 0, "note": "no staged writes for txn"}

    # Strong consistency: replicate to follower FIRST. If follower is down,
    # we roll back the staging area and fail the commit so the leader does
    # not diverge from the follower.
    if FOLLOWER_URL:
        try:
            r = httpx.post(
                f"{FOLLOWER_URL}/replicate",
                json={"updates": updates},
                timeout=5.0,
            )
            if r.status_code != 200:
                staged[req.txn_id] = updates  # restore
                raise HTTPException(
                    status_code=500,
                    detail=f"follower rejected replicate: {r.status_code} {r.text}",
                )
        except httpx.RequestError as e:
            staged[req.txn_id] = updates
            raise HTTPException(status_code=500, detail=f"follower unavailable: {e}")

    # Only after follower ACK do we apply locally.
    committed.update(updates)
    return {"ok": True, "applied": len(updates), "node": NODE_ID}


@app.post("/abort")
def abort(req: TxnReq):
    staged.pop(req.txn_id, None)
    return {"ok": True, "node": NODE_ID}


@app.post("/replicate")
def replicate(req: ReplicateReq):
    # Followers blindly apply; a real system would validate lineage / log idx.
    committed.update(req.updates)
    return {"ok": True, "applied": len(req.updates), "node": NODE_ID}
