#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x ".venv311/bin/python" ]]; then
    PYTHON_BIN=".venv311/bin/python"
  elif [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi
OUT_DIR="${OUT_DIR:-eval-results/$(date +%Y%m%d-%H%M%S)}"
CLIENTS="${CLIENTS:-8}"
READ_CLIENTS="${READ_CLIENTS:-16}"
REQUESTS="${REQUESTS:-100}"
READ_REQUESTS="${READ_REQUESTS:-500}"
TXN_READ_REQUESTS="${TXN_READ_REQUESTS:-200}"
CONTENTION_REQUESTS="${CONTENTION_REQUESTS:-50}"

mkdir -p "$OUT_DIR"

echo "Writing eval results to: $OUT_DIR"
echo "Base URL: $BASE_URL"
echo

run_eval() {
  local name="$1"
  shift
  echo "==> $name"
  echo "    $PYTHON_BIN evals.py $*"
  $PYTHON_BIN evals.py --base-url "$BASE_URL" "$@" | tee "$OUT_DIR/$name.json"
  echo
}

curl -fsS "$BASE_URL/cluster" > "$OUT_DIR/cluster-before.json"
curl -fsS "$BASE_URL/replication-lag" > "$OUT_DIR/replication-lag-before.json" || true

run_eval baseline-mixed baseline \
  --workload mixed \
  --clients "$CLIENTS" \
  --requests "$REQUESTS"

run_eval baseline-follower-reads baseline \
  --workload reads \
  --clients "$READ_CLIENTS" \
  --requests "$READ_REQUESTS"

run_eval baseline-transactional-reads baseline \
  --workload txn-reads \
  --clients "$CLIENTS" \
  --requests "$TXN_READ_REQUESTS"

run_eval baseline-single-write-txns baseline \
  --workload writes \
  --clients "$CLIENTS" \
  --requests "$REQUESTS"

run_eval baseline-multi-write-txns baseline \
  --workload writes \
  --clients "$CLIENTS" \
  --requests "$REQUESTS" \
  --writes-per-txn 5

run_eval contention-hot-key contention \
  --clients "$CLIENTS" \
  --requests "$CONTENTION_REQUESTS" \
  --hot-key employee:hot

run_eval contention-spread-keys contention \
  --clients "$CLIENTS" \
  --requests "$REQUESTS" \
  --hot-key "" \
  --key-count 20

curl -fsS "$BASE_URL/cluster" > "$OUT_DIR/cluster-after.json"
curl -fsS "$BASE_URL/replication-lag" > "$OUT_DIR/replication-lag-after.json" || true
curl -fsS "$BASE_URL/locks" > "$OUT_DIR/locks-after.json" || true

REPORT_PATH="$($PYTHON_BIN eval_report.py "$OUT_DIR")"

echo "Done. Results saved in: $OUT_DIR"
echo "Report: $REPORT_PATH"
