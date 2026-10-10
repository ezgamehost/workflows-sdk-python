#!/usr/bin/env bash
# Runs workflows-stack's conformance harness against conformance/app.py (push) and
# conformance/worker.py (pull), at the workflows-stack checkout given (CI: .workflows-stack-rev).
#   scripts/conformance.sh <workflows-stack checkout> [harness flags, e.g. --only sync]
set -euo pipefail
stack=$(cd "${1:?usage: scripts/conformance.sh <workflows-stack checkout> [harness flags]}" && pwd)
shift
root=$(pwd)
port=${PORT:-3100}
pull_port=${PULL_PORT:-3300}
secret=whsec_$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')
mkdir -p .bin
(cd "$stack" && go build -o "$root/.bin/workflows-conformance" ./cmd/workflows-conformance)
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
CONFORMANCE_SECRET=$secret uv run --frozen uvicorn conformance.app:app --host 127.0.0.1 --port "$port" --log-level warning & pids+=($!)
args=(--app "http://127.0.0.1:$port/" --secret "$secret" --scenarios "$stack/protocol/conformance")
if [ -f conformance/worker.py ]; then
  CONFORMANCE_LIVE=http://127.0.0.1:$pull_port uv run --frozen python -m conformance.worker & pids+=($!)
  args+=(--pull-addr "127.0.0.1:$pull_port" --api-key conformance-key --pull-queue conformance)
fi
for _ in $(seq 1 100); do curl -s -o /dev/null "http://127.0.0.1:$port/" && break; sleep 0.1; done
.bin/workflows-conformance run "${args[@]}" "$@"
