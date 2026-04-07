"""Configuration helpers read from environment variables."""
import os
from typing import Dict


def get_num_shards() -> int:
    return int(os.getenv("NUM_SHARDS", "2"))


def get_shard_map() -> Dict[int, Dict[str, str]]:
    """Build {shard_id: {"leader": url, "follower": url}} from env."""
    n = get_num_shards()
    shards: Dict[int, Dict[str, str]] = {}
    for i in range(n):
        shards[i] = {
            "leader": os.getenv(f"SHARD_{i}_LEADER", f"http://shard{i}-leader:8000"),
            "follower": os.getenv(f"SHARD_{i}_FOLLOWER", f"http://shard{i}-follower:8000"),
        }
    return shards
