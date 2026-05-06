"""End-to-end demo: 2PC commit, concurrency conflict (deadlock-abort),
explicit abort. Run after `docker compose up`."""
import httpx

BASE = "http://localhost:8000"


def section(title: str) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def main():
    section("cluster topology")
    print(httpx.get(f"{BASE}/cluster").json())

    # ------------------------------------------------------------------
    section("scenario 1 — 2PC commit across two shards")
    txn = httpx.post(f"{BASE}/begin").json()["txn_id"]
    print(f"began txn: {txn}")

    pairs = [
        ("user:1", "alice"),
        ("user:2", "bob"),
        ("order:42", "pending"),
        ("order:99", "shipped"),
        ("product:abc", "widget"),
    ]
    for k, v in pairs:
        r = httpx.post(f"{BASE}/write", json={"txn_id": txn, "key": k, "value": v}).json()
        print(f"  write {k:15s} = {v:10s} -> shard {r['shard_id']}")

    print("\nstaged (read-your-own-writes):")
    for k, _ in pairs:
        r = httpx.post(f"{BASE}/read", json={"txn_id": txn, "key": k}).json()
        print(f"  {k:15s} -> {r['value']} (source={r['source']}, shard={r['shard_id']})")

    print("\ncommit (two-phase)...")
    r = httpx.post(f"{BASE}/commit", json={"txn_id": txn}).json()
    print(f"  decision      : {r['decision']}")
    print(f"  shards touched: {r['committed_shards']}")
    print(f"  prepare votes : {r['votes']}")

    print("\ncommitted reads (no txn):")
    for k, _ in pairs:
        r = httpx.post(f"{BASE}/read", json={"key": k}).json()
        print(f"  {k:15s} -> {r['value']} (source={r['source']})")

    # ------------------------------------------------------------------
    section("scenario 2 — concurrent writes on same key (deadlock-abort)")
    t1 = httpx.post(f"{BASE}/begin").json()["txn_id"]
    t2 = httpx.post(f"{BASE}/begin").json()["txn_id"]
    print(f"t1={t1[:8]}...  t2={t2[:8]}...")

    # t1 takes the X-lock by writing 'contested'.
    r = httpx.post(f"{BASE}/write", json={"txn_id": t1, "key": "contested", "value": "by-t1"})
    print(f"  t1 wrote contested = by-t1  (status={r.status_code})")

    # t2 also tries to write 'contested' — should block then deadlock-abort.
    print(f"  t2 attempts the same key (should time out & 409)...")
    r = httpx.post(f"{BASE}/write", json={"txn_id": t2, "key": "contested", "value": "by-t2"})
    print(f"  t2 status     : {r.status_code}")
    print(f"  t2 detail     : {r.json().get('detail')}")

    # t1 still owns the lock and can commit cleanly.
    r = httpx.post(f"{BASE}/commit", json={"txn_id": t1}).json()
    print(f"  t1 commit     : {r['decision']}")

    r = httpx.post(f"{BASE}/read", json={"key": "contested"}).json()
    print(f"  final value   : {r['value']} (source={r['source']})")

    # ------------------------------------------------------------------
    section("scenario 3 — explicit abort discards uncommitted data")
    t3 = httpx.post(f"{BASE}/begin").json()["txn_id"]
    httpx.post(f"{BASE}/write", json={"txn_id": t3, "key": "temp:1", "value": "X"})
    print(f"  wrote temp:1 = X in txn {t3[:8]}...")
    httpx.post(f"{BASE}/abort", json={"txn_id": t3})
    r = httpx.post(f"{BASE}/read", json={"key": "temp:1"}).json()
    print(f"  after abort, temp:1 -> {r['value']} (source={r['source']})")

    # ------------------------------------------------------------------
    section("scenario 4 — observe per-shard WAL state")
    for port in (8001, 8002, 8003, 8004):
        try:
            h = httpx.get(f"http://localhost:{port}/health").json()
            print(f"  :{port} role={h['role']:10s} committed_keys={h['committed_keys']} "
                  f"prepared_txns={h['prepared_txns']}")
        except httpx.HTTPError as e:
            print(f"  :{port} unreachable ({e})")


if __name__ == "__main__":
    main()
