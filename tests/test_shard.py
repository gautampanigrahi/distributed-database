"""Shard unit tests using FastAPI's TestClient.

We configure the shard with an empty FOLLOWER_URL so commits don't try
to replicate over the network.
"""
import os

os.environ["ROLE"] = "leader"
os.environ["SHARD_ID"] = "0"
os.environ["NODE_ID"] = "test-leader"
os.environ["FOLLOWER_URL"] = ""

from fastapi.testclient import TestClient  # noqa: E402

from shard.main import app, committed, staged  # noqa: E402

client = TestClient(app)


def setup_function(_):
    committed.clear()
    staged.clear()


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["role"] == "leader"
    assert body["shard_id"] == 0


def test_write_read_commit():
    r = client.post("/write", json={"txn_id": "t1", "key": "k", "value": "v"})
    assert r.status_code == 200

    r = client.post("/read", json={"txn_id": "t1", "key": "k"})
    assert r.status_code == 200
    assert r.json()["value"] == "v"
    assert r.json()["source"] == "staged"

    # read without txn should NOT see staged writes
    r = client.post("/read", json={"key": "k"})
    assert r.json()["value"] is None

    r = client.post("/commit", json={"txn_id": "t1"})
    assert r.status_code == 200
    assert r.json()["applied"] == 1

    r = client.post("/read", json={"key": "k"})
    assert r.json()["value"] == "v"
    assert r.json()["source"] == "committed"


def test_abort_discards_staged():
    client.post("/write", json={"txn_id": "t2", "key": "x", "value": "y"})
    client.post("/abort", json={"txn_id": "t2"})
    r = client.post("/read", json={"key": "x"})
    assert r.json()["value"] is None


def test_replicate_endpoint_applies_updates():
    r = client.post("/replicate", json={"updates": {"a": "1", "b": "2"}})
    assert r.status_code == 200
    assert client.post("/read", json={"key": "a"}).json()["value"] == "1"
    assert client.post("/read", json={"key": "b"}).json()["value"] == "2"
