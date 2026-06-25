#!/bin/bash
# Boot smoke test for the docker-based app services (qa-streamlit, notebook-server, ...).
#
# These services run dcpy + app code from the *mounted repo* at runtime (editable install in
# the compose entrypoint), so the published image is deps-only and nothing in pytest exercises
# the real entrypoint. This boots the service exactly as deployed — real entrypoint, repo
# bind-mounted — and asserts it builds, starts, and serves. That catches the runtime-wiring
# failures pytest can't: a broken entrypoint, a failed editable install, missing deps, or a
# crash on boot.
#
# It deliberately does NOT chase lazy per-page import errors — the app's imports are launcher-
# independent (see apps/qa/src, imported relative to its source root) and pytest already guards
# the app's import structure. This test guards "does the container come up and serve".
#
# The entrypoint is READ from the compose file (not re-typed) so it can't drift from deploy.
#
# Usage: apps/bash/smoke_test.sh <service>   (run from the repo root)
set -euo pipefail

SERVICE="${1:?usage: smoke_test.sh <service>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# Per-service: compose file (where the entrypoint lives), Dockerfile + build context, port,
# and the URL path that returns 200 once the server is up.
case "$SERVICE" in
  qa-streamlit)
    COMPOSE="apps/docker-compose.yml"; DOCKERFILE="apps/qa/Dockerfile"; CTX="apps/qa"
    PORT=8501; HEALTH="/qaqc/_stcore/health"
    cp admin/run_environment/constraints.txt apps/qa/constraints.txt   # qa Dockerfile copies it from its context
    ;;
  notebook-server)
    COMPOSE="apps/notebook-server/docker-compose.yml"; DOCKERFILE="apps/notebook-server/Dockerfile"; CTX="."
    PORT=8080; HEALTH="/"
    ;;
  *)
    echo "smoke_test.sh: unsupported service '$SERVICE' (add a case)"; exit 2 ;;
esac

IMAGE="smoke/$SERVICE:test"; CONTAINER="smoke-$SERVICE"
cleanup() {
  echo "::group::container logs ($CONTAINER)"; docker logs "$CONTAINER" 2>&1 || true; echo "::endgroup::"
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  rm -f apps/qa/constraints.txt
}
trap cleanup EXIT

echo "==> building $IMAGE"
docker build -t "$IMAGE" -f "$DOCKERFILE" "$CTX"

# Run what actually deploys: the entrypoint is the 3rd element of ["sh","-c","<cmd>"] in compose.
ENTRY_CMD="$(yq -r ".services.\"$SERVICE\".entrypoint[2]" "$COMPOSE")"
echo "==> entrypoint (from $COMPOSE): $ENTRY_CMD"

# Forward the service's literal env from compose (e.g. notebook-server hard-requires
# MARIMO_PASSWORD). Skip host-substituted secrets (${...}) and bare pass-throughs — the smoke
# has no values for those and the services boot/serve without them.
ENV_ARGS=()
while IFS= read -r kv; do
  case "$kv" in
    ""|*'${'*) ;;                  # empty or host-substituted -> skip
    *=*) ENV_ARGS+=(-e "$kv") ;;   # literal KEY=VALUE -> forward
  esac
done < <(yq -r ".services.\"$SERVICE\".environment[]" "$COMPOSE" 2>/dev/null || true)
[ ${#ENV_ARGS[@]} -gt 0 ] && echo "==> forwarding env: ${ENV_ARGS[*]}"

# The bind-mounted checkout is owned by the runner user but the container runs as root, so the
# editable install's git call (setuptools-scm) hits "dubious ownership". git only honors
# safe.directory from GLOBAL/system config (not -c or GIT_CONFIG_* env), so write it with
# `git config --global` before the entrypoint runs. Not needed in prod (root-owned mount).
docker run -d --name "$CONTAINER" -v "$ROOT:/repos/data-engineering" -p "$PORT:$PORT" \
  "${ENV_ARGS[@]}" \
  --entrypoint sh "$IMAGE" -c \
  "git config --global --add safe.directory /repos/data-engineering && $ENTRY_CMD"

echo "==> waiting for $SERVICE to serve (editable install + boot can take ~30-60s)"
for _ in $(seq 1 45); do
  if curl -fsS "http://localhost:$PORT$HEALTH" >/dev/null 2>&1; then
    echo "SMOKE PASS: $SERVICE built, started, and served $HEALTH"; exit 0
  fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" != "true" ]; then
    echo "SMOKE FAIL: $SERVICE container exited during boot (see logs above)"; exit 1
  fi
  sleep 2
done
echo "SMOKE FAIL: $SERVICE never served $HEALTH within timeout"; exit 1
