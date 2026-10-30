# dispatch

The work queue that routes Dagster work to HTTP-only workers (tracker epic `inv-k0d`,
this package is `inv-k0d.3`). A small FastAPI app over an **in-memory Valkey**. Dispatch
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
`DISPATCH_TERMINAL_TTL_SECONDS` (7 days). Live items never expire.

## Running it

```sh
uvicorn dispatch.app:create_app --factory
```

| env | default | |
| --- | --- | --- |
| `DISPATCH_VALKEY_URL` | `redis://localhost:6379/0` | |
| `DISPATCH_SHARED_KEY` | unset (no auth) | required in the `X-Dispatch-Key` header when set |
| `DISPATCH_LEASE_SECONDS` | 60 | heartbeat at least every third of this |
| `DISPATCH_REAP_INTERVAL_SECONDS` | 15 | 0 disables the in-process reaper |
| `DISPATCH_TERMINAL_TTL_SECONDS` | 604800 | |
| `DISPATCH_MAX_EVENTS_PER_ITEM` | 100000 | more are refused (429), never trimmed |
| `DISPATCH_KEY_PREFIX` | `dispatch:` | |
| `DISPATCH_POOL_MAX_SIZE` | 32 | Valkey connections |

### The Valkey sidecar

Dispatch is its own container app, with Valkey as a second container in it, reachable only
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

All routes except `/healthz` and `/readyz` need `X-Dispatch-Key` when a key is set.

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
of `authMode: apiKey`, `method: header`, `keyParamName: X-Dispatch-Key`.

## How it stays correct

Every state transition (enqueue, claim, heartbeat, complete, fail, events, reap, cancel)
is one Lua script in [`dispatch/lua/`](dispatch/lua/), so it is atomic: Valkey runs a
script to completion before serving anything else. No transition spans two round trips.
Every script has a `#!lua` shebang, so Valkey checks memory once before it starts rather
than aborting it halfway; the key layout is documented in
[`dispatch/store.py`](dispatch/store.py).

## Tests

```sh
docker run --rm -d --name dispatch-test-valkey -p 56379:6379 \
    -v "$PWD/apps/dispatch/valkey.conf:/etc/valkey/valkey.conf:ro" \
    valkey/valkey:9.1.3-alpine valkey-server /etc/valkey/valkey.conf \
    --bind 0.0.0.0 --protected-mode no --maxmemory 64mb
DISPATCH_TEST_VALKEY_URL=redis://localhost:56379/0 python -m pytest apps/dispatch/test -s
docker rm -f dispatch-test-valkey
```

The tests FLUSHDB that Valkey. `test_concurrency.py` holds the stress tests (64 threads,
one connection each) and a mutation check showing they catch a claim split into two steps.
