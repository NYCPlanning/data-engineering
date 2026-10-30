#!/usr/bin/env bash
# Devstack end-to-end check (tracker inv-k0d.10): Dagster -> dispatch -> mock workers ->
# S3 gateway, via Dagster Pipes, plus chaos checks.
#
#   apps/devstack/e2e/e2e.sh            # bring the stack up, run every check
#   apps/devstack/e2e/e2e.sh happy gpu  # just some checks (happy gpu kill valkey cancel)
#   E2E_DOWN=1 apps/devstack/e2e/e2e.sh # docker compose down -v at the end
#
# Each check launches a run of the demo location's 'demo_chain' (plan -> build -> publish)
# for its own partition and asserts on Dagster's records, dispatch and the gateway.
set -euo pipefail
cd "$(dirname "$0")/.."
dc() { docker compose -f docker-compose.yml "$@"; }
e2e() { dc exec -T dagster-webserver python /repo/apps/devstack/e2e/e2e.py "$@"; }
dispatch() { curl -fsS -H "X-Dispatch-Key: devstack-dispatch-key" "http://127.0.0.1:8000$1"; }
log() { printf '\n== %s\n' "$*"; }
die() { echo "FAIL: $*"; exit 1; }
jget() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }

up() {
  log "stack up"
  dc up -d --build
  for _ in $(seq 120); do
    if dc ps --format '{{.Service}} {{.Health}}' | grep -q '^dagster-webserver healthy'; then
      dc ps --format '{{.Service}} {{.State}} {{.Health}}'
      return
    fi
    sleep 2
  done
  die "dagster-webserver never became healthy"
}

# buffered, so grep -q closing the pipe early can't fail the pipeline under pipefail
worker_logs() { local l; l=$(dc logs --since 5m worker-std worker-esri); printf '%s\n' "$l"; }

# container id of a worker from its worker id (<hostname>-<pid>; hostname = container id)
container_of() { echo "${1%-*}"; }

check_happy() {
  log "happy path: demo_chain[bk]"
  run=$(e2e launch demo_chain bk)
  [ "$(e2e wait "$run" --timeout 180)" = SUCCESS ] || die "run $run did not succeed"
  report=$(e2e assert-chain "$run" bk --expect-clean-staging)
  echo "$report"
  esri=$(dc ps -q worker-esri | cut -c1-12)
  pub=$(echo "$report" | jget '["publish"]["worker"]')
  [ "$(container_of "$pub")" = "$esri" ] || die "publish ran on $pub, not worker-esri ($esri)"
  echo "OK happy: publish (caps esri) ran on worker-esri"
}

check_gpu() {
  log "starvation: gpu_train[bk] needs caps gpu, which no worker has"
  run=$(e2e launch gpu_job bk)
  e2e wait-log "$run" "[dispatch] enqueued item" --step gpu_train >/dev/null
  sleep 3
  gpu=$(dispatch "/metrics/demand?caps=gpu" | jget '["demand"]')
  std=$(dispatch "/metrics/demand" | jget '["demand"]')
  echo "demand caps=gpu: $gpu, caps=(none): $std"
  [ "$gpu" -ge 1 ] || die "gpu demand is $gpu"
  [ "$std" -eq 0 ] || die "a caps-less worker sees demand $std"
  e2e terminate "$run" >/dev/null
  [ "$(e2e wait "$run" --timeout 60)" = CANCELED ] || die "gpu run not cancelled"
  [ "$(dispatch "/metrics/demand?caps=gpu" | jget '["demand"]')" -eq 0 ] || die "gpu item still queued"
  echo "OK gpu: stayed queued with demand, cancelled with the run"
}

check_kill() {
  log "chaos: kill the worker mid-build, demo_chain[bx]"
  run=$(e2e launch demo_chain bx --build-sleep 30 --build-files 3)
  read -r item attempt worker < <(e2e wait-claimed "$run" build)
  e2e wait-log "$run" "wrote part 1/3" --step build >/dev/null
  c=$(container_of "$worker")
  echo "build item $item attempt $attempt on $worker: docker kill $c"
  t0=$(date +%s)
  docker kill "$c" >/dev/null
  read -r item2 attempt2 worker2 < <(e2e wait-claimed "$run" build --item "$item" --after "$attempt" --timeout 90)
  echo "requeued: item $item2 attempt $attempt2 on $worker2 after $(( $(date +%s) - t0 ))s"
  [ "$item2" = "$item" ] && [ "$attempt2" -gt "$attempt" ] || die "expected a new attempt of item $item"
  [ "$(e2e wait "$run" --timeout 180)" = SUCCESS ] || die "run $run did not succeed"
  report=$(e2e assert-chain "$run" bx --expect-clean-staging)
  echo "$report"
  [ "$(echo "$report" | jget '["build"]["attempt"]')" = "$attempt2" ] || die "build result not from attempt $attempt2"
  docker start "$c" >/dev/null
  echo "OK kill: lease expired, item requeued, attempt $attempt2 finished the run"
}

check_valkey() {
  log "chaos: restart valkey mid-build, demo_chain[mn]"
  run=$(e2e launch demo_chain mn --build-sleep 20 --build-files 3)
  read -r item attempt worker < <(e2e wait-claimed "$run" build)
  e2e wait-log "$run" "wrote part 1/3" --step build >/dev/null
  echo "build item $item on $worker: restarting valkey"
  dc restart valkey >/dev/null
  e2e wait-log "$run" "is gone" --step build --timeout 60 >/dev/null
  [ "$(e2e wait "$run" --timeout 180)" = SUCCESS ] || die "run $run did not succeed"
  report=$(e2e assert-chain "$run" mn --expect-clean-staging)
  echo "$report"
  worker_logs | grep -q "item $item attempt $attempt: stopping step (lost)" \
    || die "no worker aborted item $item after the restart"
  echo "OK valkey: worker aborted on 404, client re-enqueued, run completed"
}

check_cancel() {
  log "chaos: terminate the run in Dagster mid-build, demo_chain[si]"
  run=$(e2e launch demo_chain si --build-sleep 60 --build-files 3)
  read -r item attempt worker < <(e2e wait-claimed "$run" build)
  e2e wait-log "$run" "external process successfully opened" --step build >/dev/null
  t0=$(date +%s)
  e2e terminate "$run" >/dev/null
  [ "$(e2e wait "$run" --timeout 90)" = CANCELED ] || die "run $run not cancelled"
  for _ in $(seq 30); do
    status=$(dispatch "/items/$item" | jget '["status"]')
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
[ ${#checks[@]} -gt 0 ] || checks=(happy gpu kill valkey cancel)
up
for c in "${checks[@]}"; do "check_$c"; done
log "all passed: ${checks[*]}"
if [ -n "${E2E_DOWN:-}" ]; then dc down -v; fi
