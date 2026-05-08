import hashlib


def stable_hash(key: str) -> int:
    """Return a stable 256-bit integer hash of the key."""
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16)


def shard_for_key(key: str, num_shards: int) -> int:
    """Map a key to a shard id in [0, num_shards)."""
    if num_shards <= 0:
        raise ValueError("num_shards must be > 0")
    return stable_hash(key) % num_shards
