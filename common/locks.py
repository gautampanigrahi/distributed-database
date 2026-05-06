"""Strict two-phase lock manager with timeout-based deadlock handling.

Lock granularity is one item = (shard_id, key). Two compatibility modes:
    S (shared, for reads) and X (exclusive, for writes).
        S/S compatible
        anything else conflicts (X/S, S/X, X/X)

Strict 2PL: a transaction releases ALL its locks at once on commit/abort
(release_all). We do NOT build a waits-for graph — instead a waiter
that exceeds `timeout_s` raises DeadlockTimeout, the coordinator catches
it and aborts that transaction. This is the standard "presumed
deadlock" approach (textbook slide 48: "Timeout: abort Xact if it
waits too long").

Concurrency: a single Condition guards all state. Coordinator endpoints
run inside FastAPI's threadpool, so a blocking acquire() is fine.
"""
import threading
import time
from collections import defaultdict
from typing import Dict, List, Set, Tuple


class DeadlockTimeout(Exception):
    """A lock acquisition exceeded the configured timeout. The coordinator
    catches this and aborts the transaction end-to-end."""


Item = Tuple[int, str]   # (shard_id, key)
Mode = str               # "S" or "X"


class _LockState:
    """Per-item lock state. mode=='' means free."""
    __slots__ = ("mode", "holders")

    def __init__(self) -> None:
        self.mode: Mode = ""
        self.holders: Set[str] = set()


class LockManager:
    def __init__(self, timeout_s: float = 5.0):
        self.timeout_s = timeout_s
        self._cond = threading.Condition()
        self._items: Dict[Item, _LockState] = defaultdict(_LockState)
        # txn_id -> list of items it has locked (for release_all).
        self._held: Dict[str, List[Item]] = defaultdict(list)
        self.timeouts = 0  # for tests / observability

    # ---- compatibility check --------------------------------------------
    def _can_grant(self, st: _LockState, txn_id: str, mode: Mode) -> bool:
        if not st.holders:
            return True                       # free
        if st.holders == {txn_id}:
            return True                       # same txn re-acquiring (incl. upgrade)
        return mode == "S" and st.mode == "S" # S/S sharing only

    def _grant(self, item: Item, txn_id: str, mode: Mode) -> None:
        """Mutate the item to grant the requested mode to txn_id.

        Precondition: _can_grant returned True.
        """
        st = self._items[item]
        if not st.holders:
            st.mode = mode                    # first holder
        elif mode == "X":
            st.mode = "X"                     # same-txn upgrade S -> X
        # else: shared lock with other readers; mode stays "S"
        st.holders.add(txn_id)

        # Track in held list for release_all (ignore re-acquires).
        if item not in self._held[txn_id]:
            self._held[txn_id].append(item)

    # ---- public API ------------------------------------------------------
    def acquire(self, txn_id: str, item: Item, mode: Mode) -> None:
        """Block until granted, or raise DeadlockTimeout."""
        if mode not in ("S", "X"):
            raise ValueError(f"bad mode {mode!r}")
        deadline = time.monotonic() + self.timeout_s
        with self._cond:
            while True:
                st = self._items[item]
                if self._can_grant(st, txn_id, mode):
                    self._grant(item, txn_id, mode)
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.timeouts += 1
                    raise DeadlockTimeout(
                        f"txn {txn_id} timed out waiting for {mode}-lock on "
                        f"{item}; current holders={sorted(st.holders)} mode={st.mode}"
                    )
                self._cond.wait(timeout=remaining)

    def release_all(self, txn_id: str) -> None:
        """Drop every lock held by txn_id and wake all waiters."""
        with self._cond:
            for item in self._held.pop(txn_id, []):
                st = self._items.get(item)
                if not st:
                    continue
                st.holders.discard(txn_id)
                if not st.holders:
                    st.mode = ""
            self._cond.notify_all()

    # ---- introspection (debug / UI) -------------------------------------
    def snapshot(self) -> Dict:
        """Lightweight view of active locks. Safe to call from any thread."""
        with self._cond:
            items = {
                f"{sid}:{key}": {"mode": st.mode, "holders": sorted(st.holders)}
                for (sid, key), st in self._items.items() if st.holders
            }
            return {
                "items": items,
                "held_by_txn": {tx: list(map(list, items_)) for tx, items_ in self._held.items()},
                "timeouts": self.timeouts,
            }
