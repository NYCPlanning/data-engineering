# de-apps image (Azure Container Apps)

One image for the data-engineering apps: `nycplanning/build-geosupport` with this repo baked in
at `/app/data-engineering` and every app's dependencies installed (`dcpy-app-qa`,
`dcpy-app-dagster`, `dcpy-app-notebook-server`, resolved from `uv.lock`). Each app is just a
startup script in `entrypoints/`. Code changes ship by rebuilding the image; nothing is cloned or
mounted at runtime.

```
apps/docker/
├── Dockerfile               # build context is the repo root
├── Dockerfile.dockerignore  # allowlist of what gets copied in
└── entrypoints/
    ├── qa.sh                # streamlit on :8501, served at the root path
    ├── dagster.sh           # webserver on :3000 + daemon; user code from a runner if DAGSTER_CODE_SERVER_HOST is set
    ├── dagster-runner.sh    # code server on :4000 for the data-engineering location; runs execute here
    └── dagster-common.sh    # sourced by both: TEMPLATE_DIR default, dagster.yaml / SQLite fallback
```

## Running locally

From the repo root:

```bash
docker build -f apps/docker/Dockerfile -t de-apps .
docker run --rm --env-file apps/.env -p 8501:8501 de-apps apps/docker/entrypoints/qa.sh
```

Then open http://localhost:8501. See `apps/env.1pw` for the env vars the apps expect.

For the Dagster apps with the S3 gateway and Azurite, see `apps/azure-local/`.

## Publishing to ACR

`.github/workflows/azure_acr_publish.yml` builds and pushes `${AZURE_ACR_LOGIN_SERVER}/de-apps:latest`
on every push to `main` that touches what the image copies in, or on manual dispatch (which can
also build `nginx-azure`). ACR credentials come from the `de_az_acr` item in the
"Data Engineering" 1Password vault.

## Deploying to Azure Container Apps

Run one revision, one replica — streamlit sessions are in-memory:

```bash
az containerapp create \
  --name de-tools \
  --resource-group rg-azu-e2-dcp-de-nonprod \
  --environment ace-azu-e2-dcp-de-nonprod \
  --image <acr>.azurecr.io/de-apps:latest \
  --command "/app/data-engineering/apps/docker/entrypoints/qa.sh" \
  --target-port 8501 \
  --revision-mode single \
  --min-replicas 1 --max-replicas 1 \
  --env-vars AWS_S3_ENDPOINT=... BUILD_ENGINE_SERVER=... # etc.
```

Use `/_stcore/health` on port 8501 for the health probe.

The environment has the custom DNS suffix `itd-data.dcp.nycnet` with a wildcard certificate, so
every app is served at `https://<app-name>.itd-data.dcp.nycnet`: this one is
https://de-tools.itd-data.dcp.nycnet. Don't add per-app custom domains under that suffix; Azure
rejects them as already in use by the environment.

Pushing a new `:latest` doesn't by itself create a new revision. To roll out a new image:

```bash
az containerapp update --name de-tools --resource-group rg-azu-e2-dcp-de-nonprod \
  --image <acr>.azurecr.io/de-apps:latest --revision-suffix <short-sha>
```

### Dagster

Dagster runs as two apps from the same image:

- **`de-dagster`** runs the webserver and daemon (`entrypoints/dagster.sh`) and imports no user
  code. With `DAGSTER_CODE_SERVER_HOST` set it loads `apps/dagster/workspace.grpc.yaml`, a single
  `grpc_server` entry whose host and port come from `DAGSTER_CODE_SERVER_HOST` and
  `DAGSTER_CODE_SERVER_PORT` (default 4000).
- **`de-dagster-runner`** runs `dagster code-server start` for one code location,
  `data-engineering` (`apps/dagster/definitions.py`, ingest and builds merged), on port 4000. With
  the default run launcher, each run is a subprocess of this code server, so runs use this app's
  CPU, memory and disk, not the webserver's.

Without `DAGSTER_CODE_SERVER_HOST`, `dagster.sh` keeps loading `workspace.yaml` (ingest and builds
as two locations, code servers and runs in the same container). Local compose and the droplet work
that way.

Instance storage (runs, event logs, schedules) goes to Postgres when `DAGSTER_PG_URL` is set: both
entrypoints copy `apps/dagster/dagster.yaml` into `DAGSTER_HOME`. Without it, Dagster falls back to
SQLite on container disk, which every new revision wipes. Both apps need it: the run workers on the
runner write event logs to the same instance. `dagster.yaml` also caps concurrent runs at 2 (the
daemon queues the rest), and the cap only applies with Postgres storage.

| App | Command | Ingress | Size | Replicas |
|---|---|---|---|---|
| `de-dagster` | `/app/data-engineering/apps/docker/entrypoints/dagster.sh` | HTTP, port 3000, external | ~1 CPU / 2 GiB | min 1, max 1 |
| `de-dagster-runner` | `/app/data-engineering/apps/docker/entrypoints/dagster-runner.sh` | TCP, port 4000, internal to the environment | 4 CPU / 8 GiB (Consumption memory is always 2 × CPU) | min 1, max 1 |

Env vars for `de-dagster`:

| Env var | Kind | Value |
|---|---|---|
| `DAGSTER_PG_URL` | secret (`secretref:`) | `postgresql://dagster:<pw>@psql-azu-e2-dcp-de-nonprod.postgres.database.azure.com:5432/dagster?sslmode=require` |
| `DAGSTER_CODE_SERVER_HOST` | plain | `de-dagster-runner` |
| `DAGSTER_CODE_SERVER_PORT` | plain, optional | `4000` (the runner's exposed TCP port) |

Env vars for `de-dagster-runner` (what ingest reads; builds also need `BUILD_ENGINE_*`):

| Env var | Kind | Value |
|---|---|---|
| `DAGSTER_PG_URL` | secret | same as `de-dagster` |
| `AWS_S3_ENDPOINT` | plain, required | `https://de-blob-gateway.itd-data.dcp.nycnet` (with scheme); no default, an S3 call without it fails |
| `DCPY_S3_ENDPOINT_DENYLIST` | plain, optional | comma-separated hosts dcpy must never reach (subdomains included); `nyc3.digitaloceanspaces.com` at cutover |
| `AWS_ACCESS_KEY_ID` | secret | gateway access key |
| `AWS_SECRET_ACCESS_KEY` | secret | gateway secret key |
| `AWS_DEFAULT_REGION` | plain | `us-east-1`; the signing region, should match the gateway's |
| `RECIPES_BUCKET` | plain | defaults to `edm-recipes` |
| `PUBLISHING_BUCKET` | plain | e.g. `edm-publishing`; no default |
| `PRIVATE_BUCKET` | plain | defaults to `edm-private` |
| `DCPY_INGEST_RAW_CONNECTOR`, `DCPY_INGEST_DATASETS_CONNECTOR`, `DCPY_*_CONNECTOR` | plain, optional | pick a registered connector per role, e.g. `az.edm.recipes.datasets`; see `docs/dcpy/README.md` |
| `AZ_S3_ENDPOINT`, `AZ_S3_ACCESS_KEY_ID`, `AZ_S3_SECRET_ACCESS_KEY` | plain / secret, optional | registers the `az.*` connectors; only needed when one process uses both stores |
| `DCPY_LIFECYCLE_DATA_DIR` | plain, optional | local scratch for runs; defaults to `.lifecycle/` in the working directory |
| `DCPY_KEEP_STAGING` | plain, optional | `true` keeps each ingest run's staging dir (`<data dir>/ingest/<template>/<partition>/<run_id>`) for debugging; by default a pushing run deletes it when it ends, pass or fail |
| `SOCRATA_USER`, `SOCRATA_PASSWORD` | secret, optional | Socrata sources work anonymously, rate-limited |
| `AXWAY_SFTP_USER`, `AXWAY_SFTP_PASSWORD` | secret | `axway` sources (DOF PLUTO inputs) |
| `SFTP_HOST`, `SFTP_USER`, `SFTP_PRIVATE_KEY_PATH` | secret | `ginger` SFTP sources; the key is a file path, so mount it |

All S3 access in dcpy (boto3, cloudpathlib, DuckDB, pyarrow) resolves its endpoint in
`dcpy.utils.s3.resolve_endpoint` and uses path-style URLs, which the Versity gateway requires. The
endpoint is checked when a client is made, not at import, so the apps start without secrets. Object
metadata goes through `dcpy.utils.s3_metadata`: names are written lowercase with underscores
(`date_created`, `commit`, `run_url`), since Azure rejects `date-created`, and read ignoring case and
`-` vs `_`, since Azure capitalizes them and older DigitalOcean archives use hyphens. Ingest templates with an `s3` source name
their bucket directly (`edm-recipes`, `edm-private`), so the gateway needs buckets by those names.
`dcp_developments` and `dcp_housing` download from the public DigitalOcean URL and keep doing so.

```bash
az containerapp create --name de-dagster-runner \
  --resource-group rg-azu-e2-dcp-de-nonprod --environment ace-azu-e2-dcp-de-nonprod \
  --image <acr>.azurecr.io/de-apps:latest \
  --command "/app/data-engineering/apps/docker/entrypoints/dagster-runner.sh" \
  --ingress internal --transport tcp --target-port 4000 --exposed-port 4000 \
  --cpu 4 --memory 8Gi --min-replicas 1 --max-replicas 1 \
  --secrets dagster-pg-url='postgresql://...' s3-key='...' s3-secret='...' \
  --env-vars DAGSTER_PG_URL=secretref:dagster-pg-url \
    AWS_S3_ENDPOINT=https://de-blob-gateway.itd-data.dcp.nycnet AWS_DEFAULT_REGION=us-east-1 \
    AWS_ACCESS_KEY_ID=secretref:s3-key AWS_SECRET_ACCESS_KEY=secretref:s3-secret # etc.

az containerapp update --name de-dagster --resource-group rg-azu-e2-dcp-de-nonprod \
  --set-env-vars DAGSTER_PG_URL=secretref:dagster-pg-url DAGSTER_CODE_SERVER_HOST=de-dagster-runner
```

Apps in one Container Apps environment reach each other by app name. With TCP ingress the client
connects to `<app>:<exposed port>`, so the webserver dials `de-dagster-runner:4000`. TCP ingress
needs an environment on a custom VNet, and `internal` keeps the port inside the environment. gRPC also runs over
HTTP/2 ingress (`--transport http2`), but Dagster's client speaks plaintext gRPC, which TCP passes
through untouched.

Things to know:

- Redeploying the runner (new image, any revision change) kills in-flight runs. Run monitoring is
  off, so they stay "Started" until cancelled by hand (enabling `run_monitoring` in `dagster.yaml`
  would mark them failed). Roll the runner between runs.
- Start order doesn't matter: until the runner answers, the webserver shows the location as
  failed to load. Reload it from Deployment > Code locations.
- The runner holds the code. A code change only needs a new runner revision; reload the location
  in the webserver if it doesn't refresh on its own.
