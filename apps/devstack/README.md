# devstack

Local stack for the hybrid-runner / dispatch work (tracker epic `inv-k0d`). It sits beside
`apps/docker-compose*.yml` and doesn't replace the prod compose.

| service | host port | what |
|---|---|---|
| postgres (16.15) | 127.0.0.1:15432 | one server, DBs `dagster` and `dagster_dcpy` (user/pass `devstack`) |
| azurite (3.37.0) | https://127.0.0.1:10000 | Blob emulator, account `devstoreaccount1`, self-signed cert in `./.certs` |
| versitygw (v1.10.0) | http://127.0.0.1:7070 | default S3 gateway in front of Azurite (key `devstack` / `devstack-secret`) |
| s3proxy (4.1.1) | http://127.0.0.1:7071 | runner-up gateway, only with `--profile s3proxy` |
| dispatch | http://127.0.0.1:8000 | queue API (`apps/dispatch`), key `devstack-dispatch-key`, lease 15s, reaper every 2s |
| valkey (9.1.3) | | sidecar: shares dispatch's network namespace, so only dispatch's localhost reaches it |
| dcpy-dagster-webserver | http://127.0.0.1:3001 | opt-in (`--profile dcpy-dagster`): `apps/dagster`'s amd64 image and code location |
| dcpy-dagster-daemon | | opt-in, as above |

Every python service except the opt-in `dcpy-dagster-*` pair runs from one slim, arch-native
image built from [`python/`](python/) (`python:3.13-slim` plus the pins in
`python/requirements.txt`, regenerated from `uv.lock` by `python/requirements.sh`). The repo is
mounted at `/repo` and is on `PYTHONPATH`, so code changes need only a restart.

## Up / down

From the repo root:

```sh
docker compose -f apps/devstack/docker-compose.yml up -d            # everything except s3proxy
docker compose -f apps/devstack/docker-compose.yml up -d postgres azurite versitygw   # storage only
docker compose -f apps/devstack/docker-compose.yml --profile s3proxy up -d s3proxy
docker compose -f apps/devstack/docker-compose.yml --profile s3proxy down -v          # stop, wipe volumes
```

First start builds the `apps/dagster` image and `uv` syncs the dagster venv into a volume
(a few minutes); later starts reuse it. The repo is mounted, so code changes need only a restart
of the dagster services. In a git worktree the container can't see the git dir, so dcpy logs a
harmless `not a git repository` warning.

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
