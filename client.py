import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "http://localhost:8000"


def request(method, base_url, path, body=None, params=None):
    url = base_url.rstrip("/") + path
    if params:
        url += "?" + urlencode(params)

    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["content-type"] = "application/json"

    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else None
    except HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            payload = raw
        return exc.code, payload
    except URLError as exc:
        return 0, {"error": str(exc.reason)}


def print_json(payload):
    print(json.dumps(payload, indent=2, sort_keys=True))


def parse_value(value):
    try:
        return json.dumps(json.loads(value), separators=(",", ":"))
    except json.JSONDecodeError:
        return value


def main():
    parser = argparse.ArgumentParser(prog="client.py")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("begin")

    write = sub.add_parser("write")
    write.add_argument("txn_id")
    write.add_argument("key")
    write.add_argument("value")

    read = sub.add_parser("read")
    read.add_argument("key")

    read_tx = sub.add_parser("read-tx")
    read_tx.add_argument("txn_id")
    read_tx.add_argument("key")

    commit = sub.add_parser("commit")
    commit.add_argument("txn_id")

    abort = sub.add_parser("abort")
    abort.add_argument("txn_id")

    sub.add_parser("transactions")
    sub.add_parser("locks")
    sub.add_parser("cluster")

    decisions = sub.add_parser("decisions")
    decisions.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()
    base_url = args.base_url

    if args.command == "begin":
        status, payload = request("POST", base_url, "/begin")
    elif args.command == "write":
        status, payload = request("POST", base_url, "/write", {
            "txn_id": args.txn_id,
            "key": args.key,
            "value": parse_value(args.value),
        })
    elif args.command == "read":
        status, payload = request("POST", base_url, "/read", {"key": args.key})
    elif args.command == "read-tx":
        status, payload = request("POST", base_url, "/read", {
            "txn_id": args.txn_id,
            "key": args.key,
        })
    elif args.command == "commit":
        status, payload = request("POST", base_url, "/commit", {"txn_id": args.txn_id})
    elif args.command == "abort":
        status, payload = request("POST", base_url, "/abort", {"txn_id": args.txn_id})
    elif args.command == "transactions":
        status, payload = request("GET", base_url, "/transactions")
    elif args.command == "locks":
        status, payload = request("GET", base_url, "/locks")
    elif args.command == "cluster":
        status, payload = request("GET", base_url, "/cluster")
    elif args.command == "decisions":
        status, payload = request("GET", base_url, "/decisions", params={"limit": args.limit})
    else:
        parser.error(f"unknown command {args.command}")

    if status == 0:
        print_json(payload)
        sys.exit(1)

    print_json(payload)
    if status >= 400:
        sys.exit(1)


if __name__ == "__main__":
    main()
