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
    └── dagster.sh           # webserver on :3000 (/dag) + daemon (POC); Postgres storage if DAGSTER_PG_URL is set
```

## Running locally

From the repo root:

```bash
docker build -f apps/docker/Dockerfile -t de-apps .
docker run --rm --env-file apps/.env -p 8501:8501 de-apps apps/docker/entrypoints/qa.sh
```

Then open http://localhost:8501. See `apps/env.1pw` for the env vars the apps expect.

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

The `dagster` app runs `entrypoints/dagster.sh` on port 3000 (`/dag`). Instance storage (runs,
event logs, schedules) goes to Postgres when `DAGSTER_PG_URL` is set: the entrypoint copies
`apps/dagster/dagster.yaml` into `DAGSTER_HOME`. Without it, Dagster falls back to SQLite on
container disk, which every new revision wipes.

| Env var | Kind | Value |
|---|---|---|
| `DAGSTER_PG_URL` | secret (`secretref:`) | `postgresql://dagster:<pw>@psql-azu-e2-dcp-de-nonprod.postgres.database.azure.com:5432/dagster?sslmode=require` |

```bash
az containerapp secret set --name dagster --resource-group rg-azu-e2-dcp-de-nonprod \
  --secrets dagster-pg-url='postgresql://dagster:<pw>@...:5432/dagster?sslmode=require'
az containerapp update --name dagster --resource-group rg-azu-e2-dcp-de-nonprod \
  --set-env-vars DAGSTER_PG_URL=secretref:dagster-pg-url
```
