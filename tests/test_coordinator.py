"""Coordinator 2PC orchestration tests.

We don't spin up real shard servers here; instead we monkey-patch
`httpx.post` / `httpx.get` on the coordinator module to route requests
to an in-memory fake whose state we can inspect. That keeps the test
fast and lets us control prepare-vote outcomes deterministically.
"""
import importlib
import os
import tempfile
from typing import Any, Dict, List

import pytest

# Configure env BEFORE importing the coordinator.
_TMP = tempfile.mkdtemp(prefix="coord-wal-")
os.environ["NUM_SHARDS"] = "2"
os.environ["SHARD_0_LEADER"] = "http://shard0/"
os.environ["SHARD_0_FOLLOWER"] = "http://shard0f/"
os.environ["SHARD_1_LEADER"] = "http://shard1/"
os.environ["SHARD_1_FOLLOWER"] = "http://shard1f/"
os.environ["LOCK_TIMEOUT_S"] = "0.5"
os.environ["WAL_DIR"] = _TMP

from fastapi.testclient import TestClient  # noqa: E402

import coordinator.main as coord  # noqa: E402


class _FakeResponse:
    def __init__(self, status_code: int, payload: Any):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            import httpx
            raise httpx.HTTPStatusError("err", request=None, response=self)


class FakeCluster:
    """Pretends to be every shard. Records the calls received."""
    def __init__(self):
        self.calls: List[Dict[str, Any]] = []
        # Per-leader behavior tweaks. Values:
        #   "ready"     — vote ready on /prepare
        #   "no"        — vote no on /prepare
        #   "down"      — raise on any call
        self.leader_state: Dict[str, str] = {
            "shard0": "ready", "shard1": "ready",
        }
        # Per-leader committed state.
        self.committed: Dict[str, Dict[str, str]] = {"shard0": {}, "shard1": {}}
        self.in_doubt: Dict[str, List[str]] = {"shard0": [], "shard1": []}

    def _which(self, url: str) -> str:
        if "shard0" in url:
            return "shard0"
        if "shard1" in url:
            return "shard1"
        return "?"

    def post(self, url: str, json: Dict[str, Any] = None, timeout: float = 5.0):
        leader = self._which(url)
        self.calls.append({"method": "POST", "url": url, "json": json})
        state = self.leader_state.get(leader, "ready")
        if state == "down":
            import httpx
            raise httpx.RequestError("down", request=None)

        if url.endswith("/prepare"):
            if state == "no":
                return _FakeResponse(500, "no")
            self.in_doubt[leader].append(json["txn_id"])
            return _FakeResponse(200, {"vote": "ready", "node": leader, "applied": len(json.get("updates") or {})})

        if url.endswith("/commit"):
            tid = json["txn_id"]
            if tid in self.in_doubt[leader]:
                self.in_doubt[leader].remove(tid)
            return _FakeResponse(200, {"ok": True, "applied": 1, "node": leader})

        if url.endswith("/abort"):
            tid = json["txn_id"]
            if tid in self.in_doubt[leader]:
                self.in_doubt[leader].remove(tid)
            return _FakeResponse(200, {"ok": True, "node": leader})

        if url.endswith("/write"):
            return _FakeResponse(200, {"ok": True, "node": leader, "staged": {json["key"]: json["value"]}})

        if url.endswith("/read"):
            return _FakeResponse(200, {
                "key": json["key"], "value": None, "source": "missing", "node": leader
            })

        return _FakeResponse(404, "no route")

    def get(self, url: str, timeout: float = 5.0):
        leader = self._which(url)
        self.calls.append({"method": "GET", "url": url})
        if url.endswith("/health"):
            if self.leader_state.get(leader) == "down":
                import httpx
                raise httpx.RequestError("down", request=None)
            return _FakeResponse(200, {"ok": True, "role": "leader"})
        if url.endswith("/status"):
            return _FakeResponse(200, {"in_doubt_txns": list(self.in_doubt[leader])})
        return _FakeResponse(404, "no route")


@pytest.fixture
def cluster(monkeypatch):
    fake = FakeCluster()
    monkeypatch.setattr(coord.httpx, "post", fake.post)
    monkeypatch.setattr(coord.httpx, "get", fake.get)
    coord.transactions.clear()
    # Reset the lock manager and decision log between tests so cases
    # don't see stale state from a prior failure.
    coord.locks = coord.LockManager(timeout_s=coord.LOCK_TIMEOUT_S)
    coord.decision_log.truncate()
    yield fake


def _client():
    # Use TestClient without lifespan to skip the heartbeat task.
    return TestClient(coord.app, raise_server_exceptions=False)


def test_2pc_happy_path_commits(cluster):
    c = _client()
    txn = c.post("/begin").json()["txn_id"]

    # Choose two keys we know hash to different shards. We can't easily
    # predict which shard each lands on, but the routing only matters for
    # the assertion that BOTH shards saw the prepare. So write three keys.
    for i, key in enumerate(["alpha", "beta", "gamma"]):
        r = c.post("/write", json={"txn_id": txn, "key": key, "value": str(i)})
        assert r.status_code == 200

    r = c.post("/commit", json={"txn_id": txn})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["decision"] == "commit"

    # Verify a /prepare call AND a /commit call were sent for each touched shard.
    def _bases(suffix: str) -> set:
        return {
            c["url"][: -len(suffix)]
            for c in cluster.calls if c["url"].endswith(suffix)
        }
    prep_bases = _bases("/prepare")
    commit_bases = _bases("/commit")
    assert len(prep_bases) >= 1
    assert prep_bases == commit_bases  # commit phase covers exactly the prepared shards

    # Locks released.
    assert coord.locks.snapshot()["held_by_txn"] == {}


def test_2pc_aborts_when_one_shard_votes_no(cluster):
    cluster.leader_state["shard1"] = "no"
    c = _client()
    txn = c.post("/begin").json()["txn_id"]
    # Write enough keys that we hit both shards (sha256 distribution).
    for i, key in enumerate(["a", "b", "c", "d", "e", "f", "g", "h"]):
        c.post("/write", json={"txn_id": txn, "key": key, "value": str(i)})

    r = c.post("/commit", json={"txn_id": txn})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert "aborted" in detail["message"]
    # Decision log records the abort.
    decisions = [r for r in coord.decision_log.all_records() if r.get("type") == "decision"]
    assert any(d["txn_id"] == txn and d["decision"] == "abort" for d in decisions)


def test_deadlock_timeout_aborts_writer(cluster, monkeypatch):
    # Lock timeout was set to 0.5s in env above.
    c = _client()
    t1 = c.post("/begin").json()["txn_id"]
    t2 = c.post("/begin").json()["txn_id"]

    # t1 takes X-lock on a key by writing.
    r = c.post("/write", json={"txn_id": t1, "key": "shared", "value": "v1"})
    assert r.status_code == 200

    # t2 tries to write the same key — should time out and 409.
    r = c.post("/write", json={"txn_id": t2, "key": "shared", "value": "v2"})
    assert r.status_code == 409
    assert "deadlock-aborted" in r.json()["detail"]
    # t2 was cleaned up.
    assert t2 not in coord.transactions


def test_explicit_abort_releases_locks(cluster):
    c = _client()
    txn = c.post("/begin").json()["txn_id"]
    c.post("/write", json={"txn_id": txn, "key": "k1", "value": "v1"})
    assert coord.locks.snapshot()["held_by_txn"]
    c.post("/abort", json={"txn_id": txn})
    assert coord.locks.snapshot()["held_by_txn"] == {}
