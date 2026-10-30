# dagster-broker

The work queue that routes Dagster work to HTTP-only workers (tracker epic `inv-k0d`,
this package is `inv-k0d.3`). A small FastAPI app over an **in-memory Valkey**. dagster-broker
is a control plane only: it holds no credentials, and artifacts never pass through it.

## Ephemeral by design

Valkey runs with no persistence, so a restart (or a redeploy) loses every item. That is
fine because the durable truth lives elsewhere: Dagster has the runs and the Pipes
messages it has already read, and blob storage has the artifacts. Clients must treat
**404 as "the queue forgot this item"**, never as an error to crash on:

- the producer (the Dagster-side Pipes client) re-enqueues with the **same idempotency
  key**;
- a worker abandons the attempt (heartbeat, events, complete and fail all answer 404).

Finished items, with their events, attempt log and idempotency key, expire after
`DAGSTER_BROKER_TERMINAL_TTL_SECONDS` (1 day). Live items never expire; idle workers drop out
of the registry after 1 day too.

## Running it

```sh
uvicorn dagster_broker.app:create_app --factory
```

| env | default | |
| --- | --- | --- |
| `DAGSTER_BROKER_VALKEY_URL` | `redis://localhost:6379/0` | |
| `DAGSTER_BROKER_SHARED_KEY` | unset (no auth) | required in the `X-Dagster-Broker-Key` header when set |
| `DAGSTER_BROKER_LEASE_SECONDS` | 60 | heartbeat at least every third of this |
| `DAGSTER_BROKER_REAP_INTERVAL_SECONDS` | 15 | 0 disables the in-process reaper |
| `DAGSTER_BROKER_TERMINAL_TTL_SECONDS` | 86400 | finished items, their events, attempts and idempotency key |
| `DAGSTER_BROKER_MAX_EVENTS_PER_ITEM` | 100000 | more are refused (429), never trimmed |
| `DAGSTER_BROKER_KEY_PREFIX` | `dagster-broker:` | |
| `DAGSTER_BROKER_POOL_MAX_SIZE` | 32 | Valkey connections |

### The Valkey sidecar

dagster-broker is its own container app, with Valkey as a second container in it, reachable only
on localhost (no auth, no TCP ingress). Run it with [`valkey.conf`](valkey.conf):

```sh
valkey-server /etc/valkey/valkey.conf   # image valkey/valkey:9.1.3-alpine
```

The config turns persistence off (`save ""`, `appendonly no`) and sets `maxmemory` with
`maxmemory-policy noeviction`. **Both are required**: an evicting policy would silently
drop queued items under memory pressure, and no `maxmemory` lets the host OOM-kill Valkey.
`/readyz` fails unless both hold. With noeviction, a full store refuses new work cleanly:
enqueue and events answer **507** with `Retry-After`, while claim, heartbeat, complete,
fail, cancel and the reaper keep working so workers can drain it. Size `maxmemory` below
the sidecar's memory limit.

Standalone Valkey only: the scripts compute key names, which cluster mode forbids.

## API

All routes except `/healthz` and `/readyz` need `X-Dagster-Broker-Key` when a key is set.

| route | |
| --- | --- |
| `POST /items` | enqueue; 201, or 200 with the existing item for a known `idempotency_key` |
| `GET /items/{id}` | the item; 404 if unknown |
| `GET /items/{id}/events?after=&attempt=&limit=` | `{events, cursor}`, oldest first; poll with `after=cursor` |
| `POST /items/{id}/cancel` | queued: cancelled now; claimed: the worker sees `cancel` on its next heartbeat |
| `POST /work/claim` | `{worker_id, caps, affinity_run_ids}` -> an item, or 204 |
| `POST /work/{id}/heartbeat` | `{worker_id, attempt}` -> `{lease_until, cancel}` |
| `POST /work/{id}/events` | `{worker_id, attempt, kind, data}` or `{..., events: [...]}`, all or nothing |
| `POST /work/{id}/complete` | `{worker_id, attempt, result}` |
| `POST /work/{id}/fail` | `{worker_id, attempt, error, retryable}`; retryable requeues until `max_attempts`, then `dead` |
| `GET /metrics/demand?caps=a,b&min_age_seconds=` | for the KEDA metrics-api scaler, below |
| `GET /healthz`, `GET /readyz` | liveness; readiness = Valkey PING + maxmemory + noeviction |

`(id, attempt, worker_id)` is the fencing token: a heartbeat, event, complete or fail
from anyone but the current holder of that attempt gets **409**. A claim matches an item
when the item's `required_caps` are a subset of the worker's `caps`, and picks items whose
`run_id` is in `affinity_run_ids` first, then higher `priority`, then oldest. Lease expiry
is the reaper's call: a heartbeat or complete after `lease_until` but before the reaper
has run still succeeds.

### `/metrics/demand`

```json
{"demand": 3, "caps": ["big", "gdal"], "min_age_seconds": 0, "queued": 4, "claimed": 1}
```

`demand` counts queued items that a worker with `caps` could claim and that have been
queued at least `min_age_seconds` (use that for a local-first fallback: scale cloud
workers only for work local workers haven't picked up). `caps` may repeat or be
comma-separated; omitted, it means a worker with no caps. In KEDA's `metrics-api`
trigger use `valueLocation: demand`, and pass the shared key with a TriggerAuthentication
of `authMode: apiKey`, `method: header`, `keyParamName: X-Dagster-Broker-Key`.

## Client, worker and Dagster Pipes

| module | needs | |
| --- | --- | --- |
| `dagster_broker.client` | stdlib | `BrokerClient`: one method per route; 404 -> `NotFound`, 409 -> `Stale`, 503/507/connection refused -> `Unavailable` (retried with backoff for calls safe to repeat) |
| `dagster_broker.worker` | stdlib | `python -m dagster_broker.worker`: claim (caps + recent-run affinity), run `payload.command` as a subprocess in its own process group, heartbeat (kills the step on cancel / 404 / 409), forward stdout as `stdio` events, complete or fail. Env: `DAGSTER_BROKER_URL`, `DAGSTER_BROKER_SHARED_KEY`, `WORKER_CAPS`, `WORKER_ID`, `WORKER_IDLE_EXIT_SECONDS` (0 = never, for desktops), `WORKER_HEARTBEAT_SECONDS`, `WORKER_POLL_SECONDS`, `WORKER_KILL_GRACE_SECONDS`, `WORKER_WORKDIR` |
| `dagster_broker.pipes_worker` | extra `worker` (dagster-pipes) | inside the step: `open_broker_pipes()` posts Pipes messages as `pipes` events with the attempt's fencing token; `staging_prefix()` is where outputs go |
| `dagster_broker.pipes_dagster` | extra `dagster` | `PipesBrokerClient`, a Pipes client (and resource): enqueue, follow status + events, re-enqueue on 404, cancel on run termination, promote staging -> final through the S3 gateway, return the results of the attempt that finished |

The item payload the worker understands: `command` (argv), `env`, `pipes` (Pipes bootstrap
env vars), `staging_prefix` (the worker appends `/<attempt>/` and passes it as
`DAGSTER_BROKER_STAGING_PREFIX`), `retry_on_failure` (default true). See
[`apps/devstack`](../devstack/README.md) for the whole flow running in compose.

## How it stays correct

Every state transition (enqueue, claim, heartbeat, complete, fail, events, reap, cancel)
is one Lua script in [`dagster_broker/lua/`](dagster_broker/lua/), so it is atomic: Valkey runs a
script to completion before serving anything else. No transition spans two round trips.
Every script has a `#!lua` shebang, so Valkey checks memory once before it starts rather
than aborting it halfway; the key layout is documented in
[`dagster_broker/store.py`](dagster_broker/store.py).

## Tests

```sh
docker run --rm -d --name dagster-broker-test-valkey -p 56379:6379 \
    -v "$PWD/apps/dagster-broker/valkey.conf:/etc/valkey/valkey.conf:ro" \
    valkey/valkey:9.1.3-alpine valkey-server /etc/valkey/valkey.conf \
    --bind 0.0.0.0 --protected-mode no --maxmemory 64mb
DAGSTER_BROKER_TEST_VALKEY_URL=redis://localhost:56379/0 python -m pytest apps/dagster-broker/test -s
docker rm -f dagster-broker-test-valkey
```

The tests FLUSHDB that Valkey. `test_concurrency.py` holds the stress tests (64 threads,
one connection each) and a mutation check showing they catch a claim split into two steps.
