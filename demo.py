"""End-to-end demo: begin txn, write keys across shards, read, commit, verify."""
import httpx

BASE = "http://localhost:8000"


def main():
    print("=" * 60)
    print("cluster topology:")
    print(httpx.get(f"{BASE}/cluster").json())
    print("=" * 60)

    # 1. Begin transaction
    txn = httpx.post(f"{BASE}/begin").json()["txn_id"]
    print(f"began txn: {txn}")

    # 2. Write several keys - they will land on different shards via sha256 routing
    pairs = [
        ("user:1", "alice"),
        ("user:2", "bob"),
        ("order:42", "pending"),
        ("order:99", "shipped"),
        ("product:abc", "widget"),
    ]
    for k, v in pairs:
        r = httpx.post(
            f"{BASE}/write", json={"txn_id": txn, "key": k, "value": v}
        ).json()
        print(f"  write {k:15s} = {v:10s} -> shard {r['shard_id']}")

    # 3. Read-your-own-writes (inside the txn, before commit)
    print("\nstaged reads (inside txn):")
    for k, _ in pairs:
        r = httpx.post(f"{BASE}/read", json={"txn_id": txn, "key": k}).json()
        print(f"  {k:15s} -> {r['value']} (source={r['source']}, shard={r['shard_id']})")

    # 4. Commit
    print("\ncommitting...")
    r = httpx.post(f"{BASE}/commit", json={"txn_id": txn}).json()
    print(f"commit result: {r}")

    # 5. Plain (non-txn) reads after commit
    print("\ncommitted reads (no txn):")
    for k, _ in pairs:
        r = httpx.post(f"{BASE}/read", json={"key": k}).json()
        print(f"  {k:15s} -> {r['value']} (source={r['source']}, shard={r['shard_id']})")

    # 6. Abort demo
    print("\nabort demo:")
    txn2 = httpx.post(f"{BASE}/begin").json()["txn_id"]
    httpx.post(f"{BASE}/write", json={"txn_id": txn2, "key": "temp:1", "value": "X"})
    print(f"  wrote temp:1=X in txn {txn2}")
    httpx.post(f"{BASE}/abort", json={"txn_id": txn2}).json()
    r = httpx.post(f"{BASE}/read", json={"key": "temp:1"}).json()
    print(f"  after abort, temp:1 -> {r['value']} (source={r['source']})")


if __name__ == "__main__":
    main()
