# devstack

Local stack for the hybrid-runner / dagster-broker work (tracker epic `inv-k0d`). It sits beside
`apps/docker-compose*.yml` and doesn't replace the prod compose.

| service | host port | what |
|---|---|---|
| postgres (16.15) | 127.0.0.1:15432 | one server, DBs `dagster` and `dagster_dcpy` (user/pass `devstack`) |
| azurite (3.37.0) | https://127.0.0.1:10000 | Blob emulator, account `devstoreaccount1`, self-signed cert in `./.certs` |
| versitygw (v1.10.0) | http://127.0.0.1:7070 | default S3 gateway in front of Azurite (key `devstack` / `devstack-secret`) |
| s3proxy (4.1.1) | http://127.0.0.1:7071 | runner-up gateway, only with `--profile s3proxy` |
| dagster-broker | http://127.0.0.1:8000 | queue API (`apps/dagster-broker`), key `devstack-dagster-broker-key`, lease 15s, reaper every 2s |
| valkey (9.1.3) | | sidecar: shares dagster-broker's network namespace, so only dagster-broker's localhost reaches it |
| dcpy-dagster-webserver | http://127.0.0.1:3001 | opt-in (`--profile dcpy-dagster`): `apps/dagster`'s amd64 image and code location |
| dcpy-dagster-daemon | | opt-in, as above |

Every python service except the opt-in `dcpy-dagster-*` pair runs from one slim, arch-native
image built from [`python/`](python/) (`python:3.13-slim` plus the pins in
`python/requirements.txt`, regenerated from `uv.lock` by `python/requirements.sh`). The repo is
mounted at `/repo` and is on `PYTHONPATH`, so code changes need only a restart.

## Up / down

From the repo root:

```sh
docker compose -f apps/devstack/docker-compose.yml up -d            # default stack
docker compose -f apps/devstack/docker-compose.yml up -d postgres azurite versitygw   # storage only
docker compose -f apps/devstack/docker-compose.yml --profile s3proxy up -d s3proxy
docker compose -f apps/devstack/docker-compose.yml down -v          # stop, wipe volumes
```

First start builds the slim python image (about 400 MB, a minute or two); later starts reuse
it. Dagster's UI is at http://127.0.0.1:3000. The opt-in `--profile dcpy-dagster` pair builds
`apps/dagster`'s image and `uv` syncs its venv into a volume (amd64, several GB); in a git
worktree it logs a harmless `not a git repository` warning.

## Running Dagster assets on workers (Pipes over dagster-broker)

The default Dagster here loads one code location,
[`demo_location/devstack_demo`](demo_location/devstack_demo/), served by the `dagster-code`
gRPC container (shared by webserver and daemon, so the UI can terminate runs the daemon
launched). Its assets `plan -> build -> publish` (partitioned by borough) and `gpu_train` don't
compute anything in Dagster: each calls `PipesBrokerClient.run` from
[`apps/dagster-broker`](../dagster-broker/README.md), which

1. enqueues a dagster-broker item whose payload carries the step command and the Pipes context
   (inline, as the bootstrap env vars), with idempotency key `run:step:partition:retry`;
2. a worker whose caps cover the item's `required_caps` claims it and runs the command as a
   subprocess; the step opens Pipes with `dagster_broker.pipes_worker.open_broker_pipes()` and its
   messages (logs, materializations) go back as dagster-broker events, fenced by `(item, attempt,
   worker)`; stdout/stderr are forwarded too;
3. the step reads upstream outputs from the gateway and writes its own under
   `_staging/<run>/<step>/<nonce>/<attempt>/`;
4. the Dagster side polls status + events, logs live, and once dagster-broker says `done` it
   promotes that attempt's staging to `demo/<asset>/<partition>/` (server-side copies through
   versitygw), deletes the step's staging, and returns the step's `MaterializeResult` plus
   dagster-broker metadata (`broker_item`, `broker_attempt`, `worker_id`, `outputs_uri`).

Workers: `worker-std` (2 replicas, no caps) and `worker-esri` (caps `esri`; `publish` needs
it). Nobody has `gpu`, so `gpu_train` stays queued and shows up in
`curl -H 'X-Dagster-Broker-Key: devstack-dagster-broker-key' 'localhost:8000/metrics/demand?caps=gpu'`.
Workers run the code in the mounted repo (no per-SHA checkout yet, inv-k0d.9).

Failure handling, all exercised by the e2e:

| what happens | what recovers it |
|---|---|
| worker dies mid-step | its lease (15 s here) expires, the reaper requeues the item, another worker runs attempt 2; attempt 1's results are dropped, only the finishing attempt's are reported |
| Valkey restarts (queue lost) | the worker's heartbeat gets 404 and it kills the step; the Dagster side gets 404, re-enqueues with the same idempotency key and a new staging nonce |
| run terminated in Dagster | the client cancels the item; the worker sees `cancel` on its next heartbeat and kills the step's process group |

## End-to-end check

```sh
apps/devstack/e2e/e2e.sh                    # up --build, then every check (~5 min)
apps/devstack/e2e/e2e.sh happy cancel       # some checks: happy gpu kill valkey cancel
E2E_DOWN=1 apps/devstack/e2e/e2e.sh         # and down -v afterwards
```

[`e2e/e2e.sh`](e2e/e2e.sh) drives the chaos from the host (`docker kill` a worker,
`docker compose restart valkey`) and runs [`e2e/e2e.py`](e2e/e2e.py) inside the webserver
container to launch runs over GraphQL and assert on Dagster's records (run status,
per-partition materialization metadata, which attempt/worker produced it) and on the gateway
(promoted objects present, no staging left). Each check uses its own partition.

## Gateway bench

`gateway_bench/bench.py` runs the inv-k0d.12 checks (multipart upload + kill/resume, small
objects, sha256 round trips, gateway memory, CopyObject / UploadPartCopy, dcpy
`PathedStorageConnector` over the gateway) against one target. Needs the host venv
(boto3, azure-storage-blob, dcpy) and the stack up with the s3proxy profile:

```sh
python apps/devstack/gateway_bench/bench.py --target versitygw   # or s3proxy, azurite (direct baseline)
python apps/devstack/gateway_bench/bench.py --target versitygw --size-mb 64 --small 50   # quick
```

It deletes its temp files and blobs when done, but Azurite doesn't hand disk back right away:
run `down -v` after a full-size run.

Why Azurite runs HTTPS: versitygw signs copy-source SAS URLs as https-only. Over plain HTTP its
CopyObject quietly falls back to streaming the bytes through the gateway.
