#!/usr/bin/env bash
# End to end: this SDK against the real workflows-stack (api, live, engine, dispatch in one
# process, `workflows-stack all`) on Postgres, Redis and an S3 fixture, with org-api faked by
# workflows-conformance fake-org-api.
#   scripts/e2e.sh <workflows-stack checkout>
# Needs DATABASE_URL (use a database of its own: it migrates and fills it). REDIS_URL and B2_* are
# optional locally (see below); CI sets them. E2E_PORT_OFFSET shifts every port (the app 3200, fake
# org-api 4100, api 6100, live 6150, engine 6200, dispatch 6250, metrics 9090), so stacks of other
# SDKs' e2e runs on the same machine don't collide.
set -euo pipefail
stack=$(cd "${1:?usage: scripts/e2e.sh <workflows-stack checkout>}" && pwd)
root=$(pwd)
mkdir -p .bin
(cd "$stack" && go build -o "$root/.bin/workflows-stack" ./cmd/workflows-stack && go build -o "$root/.bin/workflows-conformance" ./cmd/workflows-conformance)
uv sync --frozen --quiet
py=$root/.venv/bin/python

export E2E_API_KEY=ezgh_E2Etestkey000000000000000000000000000000
export E2E_ORG=0199a3c2-5b1e-7d40-9f3a-2c8e6b1d4f70 E2E_PROJECT=0199a3c2-6c2f-7e51-8a4b-3d9f7c2e5a81
# APP_ENV=local: payloads default to the S3 test fixture on 127.0.0.1:41203 (B2_* override it, as CI
# does), the KEK and service credential have local defaults, and REDIS_URL is optional.
export APP_ENV=local REGION=us-local-1 INTERNAL_API_TOKEN=e2e-internal-token
o=${E2E_PORT_OFFSET:-0}
app_port=$((3200 + o)) org_port=$((4100 + o))
export PORT=$((6100 + o)) WORKFLOWS_LIVE_PORT=$((6150 + o)) WORKFLOWS_ENGINE_PORT=$((6200 + o)) WORKFLOWS_DISPATCH_PORT=$((6250 + o)) WORKFLOWS_METRICS_PORT=$((9090 + o))
export ORG_API_URL=http://127.0.0.1:$org_port WORKFLOWS_LIVE_URL=http://127.0.0.1:$WORKFLOWS_LIVE_PORT WORKFLOWS_EGRESS_ALLOW_LOCAL=1
export WORKFLOWS_ENGINE_ADVERTISE_URL=http://127.0.0.1:$WORKFLOWS_ENGINE_PORT WORKFLOWS_DISPATCH_URL=http://127.0.0.1:$WORKFLOWS_DISPATCH_PORT
export E2E_API=http://127.0.0.1:$PORT E2E_LIVE=$WORKFLOWS_LIVE_URL E2E_APP=http://127.0.0.1:$app_port
# Exact JSON replay (key order, NUL) needs the engine's json-column migration 0005.
if compgen -G "$stack/internal/store/migrations/0005_*" > /dev/null; then export E2E_EXACT_JSON=1; fi
E2E_STATE=$(mktemp) && rm -f "$E2E_STATE" && export E2E_STATE
pids=()
# cleanup stops everything and waits for it to exit: workflows-stack drains for up to 20 s after
# SIGTERM, and a next run must find the ports free. Whatever is still up after 30 s is killed.
cleanup() {
  [ ${#pids[@]} -eq 0 ] && return 0
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; pkill -P "$p" 2>/dev/null || true; done
  for _ in $(seq 1 150); do
    alive=0
    for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && alive=1; done
    [ "$alive" = 0 ] && return 0
    sleep 0.2
  done
  for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null || true; pkill -9 -P "$p" 2>/dev/null || true; done
}
trap cleanup EXIT
# wait_for <url> [<pid>]: until the URL answers, failing at once if the process exits.
wait_for() {
  for _ in $(seq 1 150); do
    curl -fsS -o /dev/null "$1" 2>/dev/null && return 0
    if [ -n "${2:-}" ] && ! kill -0 "$2" 2>/dev/null; then echo "the process serving $1 exited" >&2; return 1; fi
    sleep 0.2
  done
  echo "timed out waiting for $1" >&2; return 1
}
for port in $app_port $org_port $PORT $WORKFLOWS_LIVE_PORT $WORKFLOWS_ENGINE_PORT $WORKFLOWS_DISPATCH_PORT $WORKFLOWS_METRICS_PORT; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "port $port is in use (a previous run still shutting down?); free it and retry" >&2; exit 1
  fi
done

.bin/workflows-conformance fake-org-api --addr 127.0.0.1:$org_port --api-key "$E2E_API_KEY" --internal-token "$INTERNAL_API_TOKEN" & pids+=($!)
wait_for http://127.0.0.1:$org_port/health
.bin/workflows-stack migrate
.bin/workflows-stack all > .bin/stack.log 2>&1 & stack_pid=$!; pids+=($stack_pid)
wait_for http://127.0.0.1:$PORT/ready "$stack_pid" || { tail -n 200 .bin/stack.log; exit 1; }

read -r E2E_APP_ID EZGH_WORKFLOWS_SIGNING_SECRET E2E_PULL_APP_ID < <("$py" -m e2e.setup) || { tail -n 100 .bin/stack.log; exit 1; }
export E2E_APP_ID EZGH_WORKFLOWS_SIGNING_SECRET E2E_PULL_APP_ID

# The app runs under a supervisor: e2e-crash exits it mid-step on purpose.
# Its TERM trap stops the app too, so a stopped supervisor never leaves it running.
(
  trap 'kill "$child" 2>/dev/null; exit 0' TERM
  while true; do
    "$py" -m uvicorn e2e.app:app --host 127.0.0.1 --port "$app_port" --log-level warning & child=$!
    wait "$child" || true
    sleep 0.2
  done
) & pids+=($!)
wait_for http://127.0.0.1:$app_port/__e2e/state
# The pull worker for the pull app (e2e-pulled).
"$py" -m e2e.worker > .bin/worker.log 2>&1 & pids+=($!)

"$py" -m e2e.run || { echo "--- stack log"; tail -n 300 .bin/stack.log; echo "--- worker log"; tail -n 50 .bin/worker.log; exit 1; }
