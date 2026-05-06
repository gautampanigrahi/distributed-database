"""Lock manager tests — strict 2PL semantics + timeout-based deadlock."""
import threading
import time

import pytest

from common.locks import DeadlockTimeout, LockManager


def test_shared_lock_compatible():
    lm = LockManager(timeout_s=1.0)
    lm.acquire("t1", (0, "a"), "S")
    lm.acquire("t2", (0, "a"), "S")  # multiple readers OK
    snap = lm.snapshot()
    assert "0:a" in snap["items"]
    assert set(snap["items"]["0:a"]["holders"]) == {"t1", "t2"}


def test_exclusive_blocks_shared_until_timeout():
    lm = LockManager(timeout_s=0.2)
    lm.acquire("writer", (0, "a"), "X")
    with pytest.raises(DeadlockTimeout):
        lm.acquire("reader", (0, "a"), "S")
    assert lm.timeouts == 1


def test_release_all_wakes_waiters():
    lm = LockManager(timeout_s=2.0)
    lm.acquire("writer", (0, "a"), "X")

    acquired = []

    def waiter():
        try:
            lm.acquire("reader", (0, "a"), "S")
            acquired.append("ok")
        except DeadlockTimeout:
            acquired.append("timeout")

    th = threading.Thread(target=waiter)
    th.start()
    time.sleep(0.1)               # let waiter block
    lm.release_all("writer")
    th.join(timeout=1.0)
    assert acquired == ["ok"]


def test_same_txn_can_upgrade_s_to_x():
    lm = LockManager(timeout_s=0.5)
    lm.acquire("t1", (0, "a"), "S")
    lm.acquire("t1", (0, "a"), "X")   # upgrade by same txn — should succeed
    assert lm.snapshot()["items"]["0:a"]["mode"] == "X"


def test_two_txn_deadlock_one_times_out():
    """Classic deadlock: t1 holds A, wants B; t2 holds B, wants A.
    With timeout=0.3s, at least one of them aborts via DeadlockTimeout."""
    lm = LockManager(timeout_s=0.3)
    lm.acquire("t1", (0, "A"), "X")
    lm.acquire("t2", (0, "B"), "X")

    aborted = []

    def t1_wants_b():
        try:
            lm.acquire("t1", (0, "B"), "X")
        except DeadlockTimeout:
            aborted.append("t1")

    def t2_wants_a():
        try:
            lm.acquire("t2", (0, "A"), "X")
        except DeadlockTimeout:
            aborted.append("t2")

    th1 = threading.Thread(target=t1_wants_b)
    th2 = threading.Thread(target=t2_wants_a)
    th1.start(); th2.start()
    th1.join(timeout=2.0); th2.join(timeout=2.0)

    # Both should time out (true deadlock — neither has anything to wake).
    assert len(aborted) >= 1
