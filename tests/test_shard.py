import importlib
import os
import tempfile

import pytest

# Configure environment BEFORE importing the shard module (it reads env
# at import time to set ROLE / WAL paths).
_TMP = tempfile.mkdtemp(prefix="shard-wal-")
os.environ["ROLE"] = "leader"
os.environ["SHARD_ID"] = "0"
os.environ["NODE_ID"] = "test-leader"
os.environ["FOLLOWER_URL"] = ""
os.environ["WAL_DIR"] = _TMP

from fastapi.testclient import TestClient  # noqa: E402

import shard.main as shard_main  # noqa: E402

client = TestClient(shard_main.app)


def setup_function(_):
    shard_main.committed.clear()
    shard_main.staged.clear()
    shard_main.prepared.clear()
    shard_main.wal.truncate()


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["role"] == "leader"
    assert body["shard_id"] == 0


def test_write_read_commit_legacy_path():
    """Backwards compat: commit without explicit /prepare still works."""
    r = client.post("/write", json={"txn_id": "t1", "key": "k", "value": "v"})
    assert r.status_code == 200

    r = client.post("/read", json={"txn_id": "t1", "key": "k"})
    assert r.json()["value"] == "v"
    assert r.json()["source"] == "staged"

    # Read without txn should NOT see staged.
    assert client.post("/read", json={"key": "k"}).json()["value"] is None

    r = client.post("/commit", json={"txn_id": "t1"})
    assert r.status_code == 200
    assert r.json()["applied"] == 1
    assert client.post("/read", json={"key": "k"}).json()["source"] == "committed"


def test_prepare_then_commit_2pc_path():
    client.post("/write", json={"txn_id": "t-2pc", "key": "x", "value": "1"})
    r = client.post("/prepare", json={"txn_id": "t-2pc"})
    assert r.status_code == 200
    assert r.json()["vote"] == "ready"

    # Status should now report this txn as in-doubt.
    assert "t-2pc" in client.get("/status").json()["in_doubt_txns"]

    r = client.post("/commit", json={"txn_id": "t-2pc"})
    assert r.status_code == 200
    assert client.post("/read", json={"key": "x"}).json()["value"] == "1"
    assert "t-2pc" not in client.get("/status").json()["in_doubt_txns"]


def test_abort_after_prepare_clears_in_doubt():
    client.post("/write", json={"txn_id": "t-ab", "key": "y", "value": "9"})
    client.post("/prepare", json={"txn_id": "t-ab"})
    assert "t-ab" in client.get("/status").json()["in_doubt_txns"]

    client.post("/abort", json={"txn_id": "t-ab"})
    assert "t-ab" not in client.get("/status").json()["in_doubt_txns"]
    assert client.post("/read", json={"key": "y"}).json()["value"] is None


def test_replicate_endpoint_applies_updates():
    r = client.post("/replicate", json={"updates": {"a": "1", "b": "2"}})
    assert r.status_code == 200
    assert client.post("/read", json={"key": "a"}).json()["value"] == "1"


def test_promote_follower_to_leader():
    """A follower-role node refuses writes, then accepts after /promote."""
    # Re-import the module with ROLE=follower into a fresh app instance.
    os.environ["ROLE"] = "follower"
    os.environ["NODE_ID"] = "test-follower"
    os.environ["FOLLOWER_URL"] = ""
    follower_tmp = tempfile.mkdtemp(prefix="shard-wal-")
    os.environ["WAL_DIR"] = follower_tmp
    importlib.reload(shard_main)
    fclient = TestClient(shard_main.app)

    # writes rejected pre-promotion
    r = fclient.post("/write", json={"txn_id": "fx", "key": "p", "value": "q"})
    assert r.status_code == 403

    rp = fclient.post("/promote", json={"new_follower_url": None})
    assert rp.status_code == 200
    assert rp.json()["promoted"] == "test-follower"

    # writes accepted post-promotion
    r = fclient.post("/write", json={"txn_id": "fx", "key": "p", "value": "q"})
    assert r.status_code == 200

    # Restore original module state for subsequent tests.
    os.environ["ROLE"] = "leader"
    os.environ["NODE_ID"] = "test-leader"
    os.environ["WAL_DIR"] = _TMP
    importlib.reload(shard_main)


def test_wal_recovery_in_doubt_txn():
    """A prepare without a matching commit should be replayed as in-doubt."""
    client.post("/write", json={"txn_id": "tin", "key": "z", "value": "Z"})
    client.post("/prepare", json={"txn_id": "tin"})

    # Simulate restart by reloading the shard module.
    importlib.reload(shard_main)
    rclient = TestClient(shard_main.app)
    assert "tin" in rclient.get("/status").json()["in_doubt_txns"]
    # And value is not yet committed.
    assert rclient.post("/read", json={"key": "z"}).json()["value"] is None
