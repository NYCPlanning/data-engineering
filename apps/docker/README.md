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
    ├── qa.sh                # streamlit on :8501, base path /qaqc
    └── dagster.sh           # webserver on :3000 (/dag) + daemon (POC)
```

## Running locally

From the repo root:

```bash
docker build -f apps/docker/Dockerfile -t de-apps .
docker run --rm --env-file apps/.env -p 8501:8501 de-apps apps/docker/entrypoints/qa.sh
```

Then open http://localhost:8501/qaqc. See `apps/env.1pw` for the env vars the apps expect.

## Publishing to ACR

`.github/workflows/azure_acr_publish.yml` builds and pushes `${AZURE_ACR_LOGIN_SERVER}/de-apps:latest`
on every push to `main` that touches what the image copies in, or on manual dispatch (which can
also build `nginx-azure`). ACR credentials come from the `de_az_acr` item in the
"Data Engineering" 1Password vault.

## Deploying to Azure Container Apps

Run one revision, one replica — streamlit sessions are in-memory:

```bash
az containerapp create \
  --name qa-streamlit \
  --resource-group <rg> \
  --environment <env> \
  --image <acr>.azurecr.io/de-apps:latest \
  --command "/app/data-engineering/apps/docker/entrypoints/qa.sh" \
  --target-port 8501 \
  --revision-mode single \
  --min-replicas 1 --max-replicas 1 \
  --env-vars AWS_S3_ENDPOINT=... BUILD_ENGINE_SERVER=... # etc.
```

Use `/qaqc/_stcore/health` for the health probe. The name `qa-streamlit` matches what
`nginx_azure` proxies `/qaqc` to.

Pushing a new `:latest` doesn't by itself create a new revision. To roll out a new image:

```bash
az containerapp update --name qa-streamlit --resource-group <rg> \
  --image <acr>.azurecr.io/de-apps:latest --revision-suffix <short-sha>
```
