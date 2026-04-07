"""Coordinator: single client-facing API that routes to shards.

Routing:   shard_id = sha256(key) % num_shards
Txns:      coordinator tracks which shards each txn touched.

TODO(sprint2): two-phase commit for multi-shard atomicity,
               leader re-election / retry on shard failure,
               client-visible snapshot isolation.
"""
import uuid
from typing import Dict, Optional, Set

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common.config import get_num_shards, get_shard_map
from common.hashing import shard_for_key

NUM_SHARDS = get_num_shards()
SHARDS = get_shard_map()

app = FastAPI(title="Coordinator")

# txn_id -> {"shards": set[int]}
transactions: Dict[str, Dict] = {}


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


def _leader(shard_id: int) -> str:
    return SHARDS[shard_id]["leader"]


# ------------------------------------------------------------------ endpoints
@app.get("/cluster")
def cluster():
    return {
        "num_shards": NUM_SHARDS,
        "shards": SHARDS,
        "hash_scheme": "sha256(key) % num_shards",
        "active_txns": len(transactions),
    }


@app.post("/begin")
def begin():
    txn_id = str(uuid.uuid4())
    transactions[txn_id] = {"shards": set()}
    return {"txn_id": txn_id}


@app.post("/write")
def write(req: WriteReq):
    if req.txn_id not in transactions:
        raise HTTPException(status_code=400, detail="unknown txn_id; call /begin first")
    sid = shard_for_key(req.key, NUM_SHARDS)
    url = _leader(sid)
    try:
        r = httpx.post(f"{url}/write", json=req.model_dump(), timeout=5.0)
        r.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"shard {sid} write failed: {e}")
    transactions[req.txn_id]["shards"].add(sid)
    return {"ok": True, "shard_id": sid, "result": r.json()}


@app.post("/read")
def read(req: ReadReq):
    sid = shard_for_key(req.key, NUM_SHARDS)
    url = _leader(sid)
    try:
        r = httpx.post(f"{url}/read", json=req.model_dump(), timeout=5.0)
        r.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"shard {sid} read failed: {e}")
    out = r.json()
    out["shard_id"] = sid
    return out


@app.post("/commit")
def commit(req: TxnReq):
    if req.txn_id not in transactions:
        raise HTTPException(status_code=400, detail="unknown txn_id")
    touched: Set[int] = transactions[req.txn_id]["shards"]

    # TODO(sprint2): replace this best-effort loop with real 2PC
    # (prepare phase across all shards, then commit phase). Today,
    # if shard N fails after shard N-1 succeeded we are left partial.
    results = {}
    failed = []
    for sid in touched:
        url = _leader(sid)
        try:
            r = httpx.post(
                f"{url}/commit", json={"txn_id": req.txn_id}, timeout=10.0
            )
            if r.status_code != 200:
                failed.append({"shard": sid, "error": r.text})
            else:
                results[sid] = r.json()
        except httpx.HTTPError as e:
            failed.append({"shard": sid, "error": str(e)})

    del transactions[req.txn_id]
    if failed:
        raise HTTPException(
            status_code=500,
            detail={"message": "commit failed", "failed": failed, "partial": results},
        )
    return {"ok": True, "committed_shards": sorted(touched), "results": results}


@app.post("/abort")
def abort(req: TxnReq):
    if req.txn_id not in transactions:
        raise HTTPException(status_code=400, detail="unknown txn_id")
    touched = transactions[req.txn_id]["shards"]
    for sid in touched:
        url = _leader(sid)
        try:
            httpx.post(f"{url}/abort", json={"txn_id": req.txn_id}, timeout=5.0)
        except httpx.HTTPError:
            pass  # best-effort; abort is idempotent
    del transactions[req.txn_id]
    return {"ok": True, "aborted_shards": sorted(touched)}
