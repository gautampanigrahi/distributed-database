"""Configuration helpers read from environment variables."""
import os
from typing import Any, Dict, List


def get_num_shards() -> int:
    return int(os.getenv("NUM_SHARDS", "2"))


def _csv_urls(value: str) -> List[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def get_shard_map() -> Dict[int, Dict[str, Any]]:
    """Build shard routing metadata from env."""
    n = get_num_shards()
    shards: Dict[int, Dict[str, Any]] = {}
    for i in range(n):
        followers_env = os.getenv(f"SHARD_{i}_FOLLOWERS")
        if followers_env is None:
            followers = _csv_urls(os.getenv(
                f"SHARD_{i}_FOLLOWER",
                f"http://shard{i}-follower:8000",
            ))
        else:
            followers = _csv_urls(followers_env)
        shards[i] = {
            "leader": os.getenv(f"SHARD_{i}_LEADER", f"http://shard{i}-leader:8000"),
            "followers": followers,
            "follower": followers[0] if followers else "",
        }
    return shards
