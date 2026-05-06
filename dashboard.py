"""Live cluster dashboard. Run on the host while `docker compose up` runs.

Polls the coordinator (cluster / transactions / locks / decisions) and
each shard node (health) every REFRESH_S seconds, and renders a
multi-panel terminal view. Designed to be the focal point of the live
demo:

  * 2PC commit:   both shard panels' `committed` counter ticks up
                  together; a row appears in `recent decisions`.
  * Concurrency:  the locks panel shows the X-holder; conflicting
                  attempts disappear with a `deadlock-aborted` 409.
  * Failover:     stop a leader container — its panel turns red, the
                  follower's role flips to leader (panel turns green)
                  about LEADER_FAIL_THRESHOLD * HEARTBEAT_INTERVAL_S
                  seconds later.

No state of its own. Quit with Ctrl-C.
"""
import time
from typing import Any, Dict, Optional

import httpx
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# ---------------------------------------------------------------- config
COORDINATOR = "http://localhost:8000"
NODES = [
    ("shard0-leader",   "http://localhost:8001"),
    ("shard0-follower", "http://localhost:8002"),
    ("shard1-leader",   "http://localhost:8003"),
    ("shard1-follower", "http://localhost:8004"),
]
REFRESH_S = 0.5
HTTP_TIMEOUT = 0.4   # tight: we'd rather show "unreachable" than block the loop


# ---------------------------------------------------------------- io
def fetch(url: str) -> Optional[Dict[str, Any]]:
    """GET url; return parsed JSON or None on any failure."""
    try:
        r = httpx.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json()
    except httpx.HTTPError:
        pass
    return None


# ---------------------------------------------------------------- panels
def coordinator_panel() -> Panel:
    info = fetch(f"{COORDINATOR}/cluster")
    if not info:
        return Panel(Text("unreachable", style="bold red"),
                     title="coordinator", border_style="red")

    body = Text()
    body.append(f"shards            : {info['num_shards']}\n", style="bold")
    body.append(f"active txns       : {info['active_txns']}\n")
    body.append(f"hash scheme       : {info['hash_scheme']}\n", style="dim")
    body.append(f"heartbeat         : {info['heartbeat_interval_s']}s\n", style="dim")
    body.append(f"lock timeout      : {info['lock_timeout_s']}s\n", style="dim")
    body.append(f"failover threshold: {info['leader_fail_threshold']}", style="dim")
    return Panel(body, title="coordinator (router + tx coordinator)",
                 border_style="green", padding=(0, 1))


def node_panel(name: str, base: str) -> Panel:
    h = fetch(f"{base}/health")
    if not h:
        return Panel(Text("unreachable", style="bold red"),
                     title=name, border_style="red")

    role = h.get("role", "?")
    prepared = h.get("prepared_txns", 0)
    # green = healthy leader; cyan = healthy follower; yellow = in-doubt txns
    border = "green" if role == "leader" else "cyan"
    if prepared > 0:
        border = "yellow"

    body = Text()
    body.append("role        : ")
    body.append(f"{role}\n", style=f"bold {border}")
    body.append(f"committed   : {h.get('committed_keys', 0)}\n")
    body.append(f"open txns   : {h.get('open_txns', 0)}\n")
    body.append("prepared    : ")
    body.append(f"{prepared}\n",
                style="bold yellow" if prepared > 0 else "default")
    follower = h.get("follower_url")
    if follower:
        body.append(f"replicates →: {follower.split('//')[-1]}", style="dim")
    return Panel(body, title=name, border_style=border, padding=(0, 1))


def transactions_table() -> Panel:
    txns = fetch(f"{COORDINATOR}/transactions") or {}
    t = Table(expand=True, show_edge=False, show_lines=False)
    t.add_column("txn", style="cyan", no_wrap=True)
    t.add_column("shards")
    t.add_column("writes")

    if not txns:
        t.add_row("—", "—", Text("no active txns", style="dim"))
    else:
        for tid, info in txns.items():
            writes = ", ".join(
                f"s{sid}:{k}={v}"
                for sid, kv in info.get("updates", {}).items()
                for k, v in kv.items()
            )
            t.add_row(tid[:8], str(info.get("shards", [])), writes or "—")
    return Panel(t, title="active transactions", border_style="white")


def locks_panel() -> Panel:
    snap = fetch(f"{COORDINATOR}/locks") or {}
    items = snap.get("items", {})
    timeouts = snap.get("timeouts", 0)

    t = Table(expand=True, show_edge=False, show_lines=False)
    t.add_column("item", style="white", no_wrap=True)
    t.add_column("mode")
    t.add_column("holders")

    if not items:
        t.add_row("—", "—", Text("no locks held", style="dim"))
    else:
        for item, info in items.items():
            mode = info.get("mode", "?")
            mode_style = "bold red" if mode == "X" else "bold blue"
            holders = ", ".join(h[:8] for h in info.get("holders", []))
            t.add_row(item, Text(mode, style=mode_style), holders)

    title = f"lock manager  (deadlock-timeouts: {timeouts})"
    return Panel(t, title=title, border_style="white")


def decisions_panel() -> Panel:
    data = fetch(f"{COORDINATOR}/decisions?limit=8") or {"decisions": []}
    decisions = data.get("decisions", [])

    t = Table(expand=True, show_edge=False, show_lines=False)
    t.add_column("txn", style="cyan", no_wrap=True)
    t.add_column("decision")
    t.add_column("shards")
    t.add_column("reason", style="dim")

    if not decisions:
        t.add_row("—", "—", "—", "no 2PC decisions yet")
    else:
        for rec in decisions:
            d = rec.get("decision", "?")
            style = "bold green" if d == "commit" else "bold red"
            t.add_row(
                rec.get("txn_id", "?")[:8],
                Text(d, style=style),
                str(rec.get("participants", [])),
                rec.get("reason", "—"),
            )
    return Panel(t, title="recent 2PC decisions (newest last)", border_style="white")


# ---------------------------------------------------------------- layout
def build_layout() -> Layout:
    """Top: coordinator. Middle: 4 shard nodes side by side.
    Bottom: txns + locks + decisions."""
    root = Layout()
    root.split_column(
        Layout(name="head", size=9),
        Layout(name="nodes", size=11),
        Layout(name="bottom"),
    )
    root["head"].update(Panel(Text("loading…", style="dim"),
                              title="cluster", border_style="dim"))

    nodes_row = root["nodes"]
    nodes_row.split_row(*[Layout(name=f"node{i}") for i in range(len(NODES))])

    bottom = root["bottom"]
    bottom.split_row(
        Layout(name="txns", ratio=2),
        Layout(name="locks", ratio=2),
        Layout(name="decisions", ratio=3),
    )
    return root


def render(layout: Layout) -> None:
    layout["head"].update(coordinator_panel())
    for i, (name, base) in enumerate(NODES):
        layout[f"node{i}"].update(node_panel(name, base))
    layout["txns"].update(transactions_table())
    layout["locks"].update(locks_panel())
    layout["decisions"].update(decisions_panel())


# ---------------------------------------------------------------- main
def main() -> None:
    console = Console()
    layout = build_layout()
    refresh_hz = max(1, int(1 / REFRESH_S))

    with Live(layout, console=console, screen=True,
              refresh_per_second=refresh_hz) as live:
        try:
            while True:
                render(layout)
                time.sleep(REFRESH_S)
        except KeyboardInterrupt:
            pass
    console.print("[dim]dashboard stopped.[/dim]")


if __name__ == "__main__":
    main()
