"""WAL primitive tests."""
import os
import tempfile

from common.wal import WAL, find_last


def test_append_replay_roundtrip(tmp_path):
    w = WAL(str(tmp_path / "x.wal"))
    w.append({"type": "prepare", "txn_id": "t1", "updates": {"k": "v"}})
    w.append({"type": "commit", "txn_id": "t1"})
    recs = w.all_records()
    assert len(recs) == 2
    assert recs[0]["type"] == "prepare"
    assert recs[1]["type"] == "commit"


def test_torn_line_skipped(tmp_path):
    """A torn final write (no newline) should not crash replay."""
    p = tmp_path / "x.wal"
    p.write_text('{"type":"commit","txn_id":"a"}\n{"type":"prepa')
    w = WAL(str(p))
    recs = w.all_records()
    assert len(recs) == 1
    assert recs[0]["txn_id"] == "a"


def test_find_last(tmp_path):
    w = WAL(str(tmp_path / "x.wal"))
    w.append({"type": "decision", "txn_id": "T", "decision": "commit"})
    w.append({"type": "decision", "txn_id": "T", "decision": "abort"})
    found = find_last(w.all_records(), type="decision", txn_id="T")
    assert found["decision"] == "abort"


def test_truncate(tmp_path):
    w = WAL(str(tmp_path / "x.wal"))
    w.append({"type": "x"})
    w.truncate()
    assert w.all_records() == []
