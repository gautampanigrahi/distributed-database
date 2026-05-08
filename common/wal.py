import json
import os
import threading
from typing import Any, Dict, Iterator, List, Optional


class WAL:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        # Make sure the parent dir exists, then touch the file so a
        # brand-new node can replay() without an exception.
        parent = os.path.dirname(os.path.abspath(path)) or "."
        os.makedirs(parent, exist_ok=True)
        if not os.path.exists(path):
            open(path, "a").close()

    def append(self, record: Dict[str, Any]) -> None:
        """Persist one record. Returns only after fsync — the record is durable."""
        line = json.dumps(record, sort_keys=True)
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())

    def replay(self) -> Iterator[Dict[str, Any]]:
        """Yield records in append order. Bad lines (torn writes) are skipped."""
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue   # tail-of-file torn write; keep going

    def all_records(self) -> List[Dict[str, Any]]:
        return list(self.replay())

    def truncate(self) -> None:
        """Wipe the log. Test-only; production code never calls this."""
        with self._lock:
            open(self.path, "w").close()


def find_last(records: List[Dict[str, Any]], **match) -> Optional[Dict[str, Any]]:
    """Return the last record where every key in `match` equals. Used by tests."""
    for rec in reversed(records):
        if all(rec.get(k) == v for k, v in match.items()):
            return rec
    return None
