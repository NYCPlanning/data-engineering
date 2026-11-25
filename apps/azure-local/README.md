# Azure Dagster stack, locally

A docker compose copy of the Azure Dagster deployment (see `apps/docker/README.md`), with Azurite
standing in for the storage account:

| Service | Azure counterpart | Notes |
|---|---|---|
| `de-dagster` | `de-dagster` app | `entrypoints/dagster.sh`, loads code over gRPC from `de-dagster-runner`; http://127.0.0.1:3000 |
| `de-dagster-runner` | `de-dagster-runner` app | `entrypoints/dagster-runner.sh`, code server on 4000; runs execute here |
| `de-blob-gateway` | `de-blob-gateway` app | `versity/versitygw:v1.10.0`, Azure backend; host port 127.0.0.1:17070 |
| `azurite` | storage account (flat, no HNS) | HTTPS with a self-signed cert from `certs`; host port 127.0.0.1:10000 |
| `postgres` | Dagster's Postgres flexible server | Postgres 18; host port 127.0.0.1:15432; `dagster` and `de_apps` databases |
| `db-init` | - | one-shot: creates the `de_apps` role and database if missing (see `postgres-init/`) |
| `bucket-init` | - | one-shot: creates `edm-recipes` and `edm-publishing` through the gateway; no `edm-private`, as on Azure |

Both Dagster services run the `de-apps:local` image with this repo mounted over
`/app/data-engineering`. The dcpy installs are editable and point there, so a code change only
needs `docker compose restart de-dagster-runner`. Rebuild the image only when dependencies change.

All S3 traffic from the runner goes to `http://de-blob-gateway:7070` with the local keys
`local` / `local-secret`. Nothing in the stack has DigitalOcean or Azure credentials.
`DCPY_S3_ENDPOINT_DENYLIST=nyc3.digitaloceanspaces.com` makes dcpy refuse DigitalOcean outright.

The runner sets `RECIPES_BUCKET_ROOT_FOLDER=ingest`, `PRIVATE_BUCKET=edm-recipes` and
`INBOX_ROOT_FOLDER=inbox`, as on Azure. Ingest archives land in `edm-recipes/ingest/datasets/` and
`edm-recipes/ingest/raw/`, `edm.private` uses `edm-recipes/ingest/datasets/` too, and a template
whose source says `bucket: edm-private` reads `edm-recipes/inbox/<key>`. To try one, upload a csv
to `edm-recipes/inbox/qa_app/inbox/bpl_libraries_inbox_test/<version>/bpl_libraries.csv` and run
`launch_ingest.py bpl_libraries_inbox_test --version <version>`. Objects at `datasets/`, `raw/` or
in `edm-private` from earlier runs are left alone and no longer read.

That's the default: plain `AWS_*` pointed at the gateway, as on Azure. To run the runner the way a
process that reads DigitalOcean and writes Azure would, add the overlay
`docker-compose.az-connectors.yml`: ingest writes through the `az.*` connectors (`AZ_S3_*` ->
gateway), and `AWS_S3_ENDPOINT` points at DigitalOcean, which the denylist refuses, so anything
still using the default connectors fails rather than leaving the stack:

```bash
docker compose -f apps/azure-local/docker-compose.yml \
  -f apps/azure-local/docker-compose.az-connectors.yml up -d de-dagster-runner
```

## Build the image

```bash
apps/azure-local/build-image.sh          # tags de-apps:local
```

This builds `apps/docker/Dockerfile` from a copy of the git-tracked and unignored files, so stray
untracked directories (an old `python/library/`, say) don't end up in the `python/*/` glob. The
image is linux/amd64 (the geosupport base) and runs under emulation on Apple silicon. A cold build
takes around 10 minutes.

## Run

```bash
docker compose -f apps/azure-local/docker-compose.yml up -d
docker compose -f apps/azure-local/docker-compose.yml ps    # wait for de-dagster-runner (healthy)
```

The webserver can come up before the runner; it shows the `data-engineering` location as failed
until the runner answers, then reloads it on its own.

Launch an ingest (for a template with a dynamic partitions def, registers the version in
`ingest_<template>` first; then launches `ingest_<template>` with `push: true`, `latest: true` and
waits for the run):

```bash
python apps/azure-local/launch_ingest.py bpl_libraries                 # version defaults to today
python apps/azure-local/launch_ingest.py bpl_libraries --version 20261124 --no-latest
python apps/azure-local/launch_ingest.py dcp_ct2020 --version 26c      # a static quarterly key
```

Templates with `version_schedule: geosupport_quarterly` share static keys (`15a` through next
year's `d`, lowercase, as Bytes names its files), so their version must be one of them.

### Archives Dagster didn't write

The `ingest_archive_sync` sensor (stopped by default; start it under Automation) reports archives
in `edm-recipes/ingest/datasets/` that an ingest asset has no materialization for, with
`source: archive_sync` in the metadata. The runner has the version catalog, so each tick checks
every template from one catalog query: materialize `version_catalog` after uploading an archive by
hand. (Without `DCPY_CATALOG_PG_URL`, each tick lists the store for the next 25, round-robin.) To
try it, upload a fake archive for a template Dagster never ran:

```bash
python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:17070', region_name='us-east-1',
                  aws_access_key_id='local', aws_secret_access_key='local-secret')
for name in ['dcp_nta2020.parquet', 'config.json']:
    s3.put_object(Bucket='edm-recipes', Key=f'ingest/datasets/dcp_nta2020/26b/{name}', Body=b'{}')
"
```

A version counts once its `config.json` is there. `latest` and `<version>_library` aren't
versions, and a quarterly template's archive that isn't a key (`26B`, say) is logged and skipped.

A tick that only reports materializations still shows as skipped ("empty result"): it started no
runs. Its log is written on the runner (`$DAGSTER_HOME/storage/__repository__/ingest_archive_sync/`),
since the sensor is evaluated in the code server.

Check what landed, through the gateway:

```bash
python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:17070', region_name='us-east-1',
                  aws_access_key_id='local', aws_secret_access_key='local-secret')
for o in s3.list_objects_v2(Bucket='edm-recipes', Prefix='ingest/datasets/bpl_libraries/')['Contents']:
    print(o['Key'])
"
```

or in Azurite directly (the cert lives in the `certs` volume):

```bash
docker compose -f apps/azure-local/docker-compose.yml exec -T azurite cat /certs/azurite.pem > /tmp/azurite.pem
python -c "
from azure.storage.blob import BlobServiceClient
svc = BlobServiceClient(
    'https://127.0.0.1:10000/devstoreaccount1', connection_verify='/tmp/azurite.pem',
    credential={'account_name': 'devstoreaccount1', 'account_key': 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='})
for b in svc.get_container_client('edm-recipes').list_blobs():
    print(b.name)
"
```

### Version catalog

The runner's `DCPY_CATALOG_PG_URL` points at the `de_apps` database (role and password
`de_apps`), created by `postgres-init/01-de-apps.sql`: at first init, and by `db-init` on every
`up`, so an older volume gets it too. As on Azure, `public` there isn't writable by the owner.
Materialize `version_catalog` (group `catalog`) to fill it from `edm-recipes/ingest/datasets/`;
ingest runs add their own archives as they go. To look:

```bash
docker compose -f apps/azure-local/docker-compose.yml exec postgres psql -U de_apps -d de_apps \
  -c "select key, version, is_latest, file_types, archived_at from catalog.versions order by 1, 2"
docker compose -f apps/azure-local/docker-compose.yml exec de-dagster-runner \
  python -m dcpy connectors catalog show --key dcp_ct2020
```

Dagster's runs and event logs are in Postgres:

```bash
docker compose -f apps/azure-local/docker-compose.yml exec postgres psql -U dagster -d dagster
```

## Stop

```bash
docker compose -f apps/azure-local/docker-compose.yml down       # keeps volumes
docker compose -f apps/azure-local/docker-compose.yml down -v    # drops Postgres, Azurite, certs
```

## Gotchas this setup handles

- Azurite serves HTTPS. versitygw signs copy-source SAS URLs as https-only, so over plain HTTP its
  CopyObject quietly falls back to streaming through the gateway.
- `NODE_EXTRA_CA_CERTS` on Azurite (it fetches copy sources from itself) and `SSL_CERT_FILE` on
  the gateway (Go reads its trusted roots from there) both point at the self-signed cert.
- `AZ_COPY_SAS_VERSION: "2025-11-05"`: Azurite lags the SAS version the gateway's SDK signs with.
- Azurite runs with `--skipApiVersionCheck --loose`.
- Runner scratch goes to `/tmp/dcpy-lifecycle` (`DCPY_LIFECYCLE_DATA_DIR`), not into the mounted
  repo. On Azure it's left unset and lands inside the image. Each ingest run deletes its own staging dir when it
  ends; set `DCPY_KEEP_STAGING: "true"` on the runner to keep them.
