#!/usr/bin/env bash
# Local end-to-end check (tracker inv-k0d.10): Dagster -> dagster-broker -> workers ->
# S3 gateway, via Dagster Pipes, plus chaos checks, using the runner_tests jobs.
#
#   apps/local/e2e/e2e.sh            # bring the stack up, run every check
#   apps/local/e2e/e2e.sh happy gpu  # just some checks (locations happy fail gpu kill valkey cancel)
#   E2E_DOWN=1 apps/local/e2e/e2e.sh # docker compose down -v at the end
set -euo pipefail
cd "$(dirname "$0")/.."
dc() { docker compose -f docker-compose.yml "$@"; }
# The dagster service holds no gateway credentials; the checks get them for this exec only.
aws=(-e AWS_S3_ENDPOINT=http://az-blob-gateway:7070 -e AWS_ACCESS_KEY_ID=local
     -e AWS_SECRET_ACCESS_KEY=local-secret -e AWS_DEFAULT_REGION=us-east-1)
e2e() { dc exec -T "${aws[@]}" dagster python /app/repos/data-engineering/apps/local/e2e/e2e.py "$@"; }
broker() { curl -fsS -H "X-Dagster-Broker-Key: local-dagster-broker-key" "http://127.0.0.1:8000$1"; }
log() { printf '\n== %s\n' "$*"; }
die() { echo "FAIL: $*"; exit 1; }
jget() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }

up() {
  log "stack up"
  dc up -d
  for _ in $(seq 300); do
    if dc ps --format '{{.Service}} {{.Health}}' | grep -q '^dagster healthy'; then
      dc ps --format '{{.Service}} {{.State}} {{.Health}}'
      return
    fi
    sleep 2
  done
  die "dagster never became healthy"
}

# buffered, so grep -q closing the pipe early can't fail the pipeline under pipefail
worker_logs() { local l; l=$(dc logs --since 5m worker-resident worker-burst worker-esri); printf '%s\n' "$l"; }

# container id of a worker from its worker id (<hostname>-<pid>-<slot>; hostname = container id)
container_of() { echo "${1%%-*}"; }
on() { local ids; ids=$(dc ps -q "$1" | cut -c1-12); grep -qx "$(container_of "$2")" <<<"$ids"; }

# run_ok <run> <service>: the run succeeded, outputs promoted, its step ran on <service>
run_ok() {
  [ "$(e2e wait "$1" --timeout 300)" = SUCCESS ] || die "run $1 did not succeed"
  report=$(e2e assert-run "$1")
  echo "$report"
  w=$(echo "$report" | jget '.popitem()[1]["worker"]')
  on "$2" "$w" || die "run $1 ran on $w, not $2"
}

check_locations() {
  log "apps/dagster/workspace.yaml loads"
  e2e locations
}

check_happy() {
  log "happy path: small on resident, standard + write on burst, esri on esri"
  # Burst workers also take small items; keep them off so small must go to resident.
  dc stop worker-burst >/dev/null
  run_ok "$(e2e launch small)" worker-resident
  dc start worker-burst >/dev/null
  standard=$(e2e launch standard); write=$(e2e launch write); esri=$(e2e launch esri)
  run_ok "$standard" worker-burst
  run_ok "$write" worker-burst
  run_ok "$esri" worker-esri
  echo "OK each case ran on its worker, outputs promoted, staging clean"
}

check_fail() {
  log "failing step: no retry, run fails"
  run=$(e2e launch failing)
  read -r item _ _ < <(e2e wait-claimed "$run" failing)
  [ "$(e2e wait "$run" --timeout 120)" = FAILURE ] || die "run $run did not fail"
  status=$(broker "/items/$item" | jget '["status"]')
  [ "$status" = failed ] || die "item $item is $status"
  echo "OK fail: item $item failed after one attempt, run failed"
}

check_gpu() {
  log "starvation: gpu needs caps gpu, which no worker has"
  run=$(e2e launch gpu)
  e2e wait-log "$run" "[dagster-broker] enqueued item" --step gpu >/dev/null
  sleep 3
  gpu=$(broker "/metrics/demand?caps=gpu" | jget '["demand"]')
  std=$(broker "/metrics/demand" | jget '["demand"]')
  echo "demand caps=gpu: $gpu, caps=(none): $std"
  [ "$gpu" -ge 1 ] || die "gpu demand is $gpu"
  [ "$std" -eq 0 ] || die "a caps-less worker sees demand $std"
  e2e terminate "$run" >/dev/null
  [ "$(e2e wait "$run" --timeout 60)" = CANCELED ] || die "gpu run not cancelled"
  [ "$(broker "/metrics/demand?caps=gpu" | jget '["demand"]')" -eq 0 ] || die "gpu item still queued"
  echo "OK gpu: stayed queued with demand, cancelled with the run"
}

check_kill() {
  log "chaos: kill the worker mid-step (slow)"
  run=$(e2e launch slow --seconds 30)
  read -r item attempt worker < <(e2e wait-claimed "$run" slow)
  e2e wait-log "$run" "wrote part 1/3" --step slow >/dev/null
  c=$(container_of "$worker")
  echo "item $item attempt $attempt on $worker: docker kill $c"
  t0=$(date +%s)
  docker kill "$c" >/dev/null
  read -r item2 attempt2 worker2 < <(e2e wait-claimed "$run" slow --item "$item" --after "$attempt" --timeout 90)
  echo "requeued: item $item2 attempt $attempt2 on $worker2 after $(( $(date +%s) - t0 ))s"
  [ "$item2" = "$item" ] && [ "$attempt2" -gt "$attempt" ] || die "expected a new attempt of item $item"
  docker start "$c" >/dev/null
  run_ok "$run" worker-burst
  [ "$(echo "$report" | jget '["slow"]["attempt"]')" = "$attempt2" ] || die "result not from attempt $attempt2"
  echo "OK kill: lease expired, item requeued, attempt $attempt2 finished the run"
}

check_valkey() {
  log "chaos: restart valkey mid-step (slow)"
  run=$(e2e launch slow --seconds 20)
  read -r item attempt worker < <(e2e wait-claimed "$run" slow)
  e2e wait-log "$run" "wrote part 1/3" --step slow >/dev/null
  echo "item $item on $worker: restarting valkey"
  dc restart valkey >/dev/null
  e2e wait-log "$run" "is gone" --step slow --timeout 60 >/dev/null
  run_ok "$run" worker-burst
  worker_logs | grep -q "item $item attempt $attempt: stopping step (lost)" \
    || die "no worker aborted item $item after the restart"
  echo "OK valkey: worker aborted on 404, client re-enqueued, run completed"
}

check_cancel() {
  log "chaos: terminate the run in Dagster mid-step (slow)"
  run=$(e2e launch slow)
  read -r item attempt worker < <(e2e wait-claimed "$run" slow)
  e2e wait-log "$run" "external process successfully opened" --step slow >/dev/null
  t0=$(date +%s)
  e2e terminate "$run" >/dev/null
  [ "$(e2e wait "$run" --timeout 90)" = CANCELED ] || die "run $run not cancelled"
  for _ in $(seq 30); do
    status=$(broker "/items/$item" | jget '["status"]')
    [ "$status" = cancelled ] && break
    sleep 1
  done
  echo "item $item: $status after $(( $(date +%s) - t0 ))s"
  [ "$status" = cancelled ] || die "item $item is $status"
  worker_logs | grep -q "item $item attempt $attempt: stopping step (cancelled)" \
    || die "worker didn't kill item $item's step"
  echo "OK cancel: Dagster termination cancelled the item and the worker killed the step"
}

checks=("$@")
[ ${#checks[@]} -gt 0 ] || checks=(locations happy fail gpu kill valkey cancel)
up
for c in "${checks[@]}"; do "check_$c"; done
log "all passed: ${checks[*]}"
if [ -n "${E2E_DOWN:-}" ]; then dc down -v; fi
