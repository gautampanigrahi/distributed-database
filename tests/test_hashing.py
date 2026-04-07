from common.hashing import shard_for_key, stable_hash


def test_stable_hash_deterministic():
    assert stable_hash("foo") == stable_hash("foo")
    assert stable_hash("user:42") == stable_hash("user:42")


def test_stable_hash_distinguishes_keys():
    assert stable_hash("foo") != stable_hash("bar")


def test_shard_for_key_in_range():
    for k in ["a", "b", "user:1", "order:42", "zzz"]:
        assert 0 <= shard_for_key(k, 4) < 4


def test_shard_for_key_distribution():
    counts = [0, 0]
    for i in range(2000):
        counts[shard_for_key(f"key{i}", 2)] += 1
    # Expect roughly balanced; allow wide margin.
    assert counts[0] > 700 and counts[1] > 700
