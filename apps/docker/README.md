# Azure Container Apps - Dynamic Code Loading Setup

This directory contains the infrastructure for deploying data-engineering apps to Azure Container Apps with dynamic code loading from the git repository.

## Architecture Overview

**Key Features:**
- Single Docker image for all apps (based on `build-geosupport:latest`)
- Dynamic code loading: Apps clone and update from git at runtime
- Hot-reload: Code updates every minute via cron, no container restarts needed
- App-specific entrypoints for different services

**How It Works:**
1. Container starts with app-specific entrypoint (e.g., `dagster.sh`)
2. Entrypoint clones the data-engineering repo
3. Installs dcpy with all app dependencies
4. Supervisord manages multiple processes:
   - App processes (webserver, daemon, etc.)
   - Reload loop (updates git repo every 60s and triggers app restart)

## Directory Structure

```
apps/docker/
├── Dockerfile              # Unified Dockerfile for all apps
├── scripts/
│   ├── update-repo.sh      # Clones or updates the git repo
│   └── reload-loop.sh      # Periodic git update + app reload
├── entrypoints/
│   └── dagster.sh          # Dagster-specific startup script
└── supervisord/
    └── dagster.conf        # Supervisord config for Dagster processes
```

## Building and Pushing to ACR

### Via GitHub Actions (Recommended)

1. Go to Actions tab in GitHub
2. Run "Azure ACR - Build and Publish App Images" workflow
3. Select which images to build/push:
   - **de-apps**: Builds and pushes the de-apps image (dagster, qa, notebook apps)
   - **nginx-azure**: Builds and pushes the nginx-azure reverse proxy image
4. Images will be pushed to:
   - `${AZURE_ACR_LOGIN_SERVER}/de-apps:latest`
   - `${AZURE_ACR_LOGIN_SERVER}/nginx-azure:latest` (if selected)

**Prerequisites:**
- 1Password vault "Data Engineering" with entry "de_az_acr" containing:
  - `username`: Azure Service Principal client ID
  - `password`: Azure Service Principal password/secret
  - `login_server`: ACR endpoint (e.g., `dcpdecontainers-c8eze3cfayagfcaq.azurecr.io`)
- GitHub secret:
  - `OP_SERVICE_ACCOUNT_TOKEN`: 1Password service account token (already configured)

### Manually

```bash
# Set your ACR name
ACR_NAME="dcpdecontainers-c8eze3cfayagfcaq.azurecr.io"

# From the apps/ directory
docker build -f docker/Dockerfile -t ${ACR_NAME}/de-apps:latest .

# Login to ACR
az acr login --name dcpdecontainers-c8eze3cfayagfcaq

# Push
docker push ${ACR_NAME}/de-apps:latest
```

## Deploying to Azure Container Apps

### Dagster Example

```bash
ACR_NAME="dcpdecontainers-c8eze3cfayagfcaq.azurecr.io"

az containerapp create \
  --name dagster \
  --resource-group <your-rg> \
  --environment <your-env> \
  --image ${ACR_NAME}/de-apps:latest \
  --command "/app/docker/entrypoints/dagster.sh" \
  --target-port 3000 \
  --env-vars \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    BUILD_ENGINE_SERVER=<your-db-conn-string> \
    AWS_S3_ENDPOINT=<your-s3-endpoint> \
    REPO_BRANCH=main \
  --cpu 2 \
  --memory 4Gi
```

### Environment Variables

**Common:**
- `REPO_URL`: Git repo URL (default: `https://github.com/NYCPlanning/data-engineering.git`)
- `REPO_BRANCH`: Branch to clone (default: `main`)
- `REPO_DIR`: Where to clone repo (default: `/app/repos/data-engineering`)
- `RELOAD_INTERVAL`: Seconds between git updates (default: `60`)

**Dagster-specific:**
- `DAGSTER_HOME`: Dagster metadata storage (default: `/opt/dagster/dagster_home`)
- `BUILD_ENGINE_SERVER`: PostgreSQL connection string
- `DCPY_LIFECYCLE_DATA_DIR`: Directory for lifecycle data
- `AWS_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`: S3 credentials
- `PUBLISHING_BUCKET`, `RECIPES_BUCKET`: S3 bucket names

## App Dependencies

All app dependencies are now managed in `pyproject.toml` under `[project.optional-dependencies]`:

- `dcpy[dagster]`: Dagster, dagster-webserver, supervisor
- `dcpy[qa]`: Streamlit, folium, mapclassify, etc.
- `dcpy[notebook]`: Marimo, uvicorn
- `dcpy[all-apps]`: All of the above

## Creating New App Entrypoints

To add a new app (e.g., QA Streamlit):

1. **Create supervisord config** (`supervisord/qa.conf`):
```ini
[supervisord]
nodaemon=true
user=root

[program:streamlit]
command=streamlit run /app/repos/data-engineering/apps/qa/src/index.py --server.port=8501 --server.address=0.0.0.0 --server.baseUrlPath=/qaqc
directory=/app/repos/data-engineering/apps/qa
autostart=true
autorestart=true
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0

[program:reload-loop]
command=/app/docker/scripts/reload-loop.sh
environment=RELOAD_COMMAND="supervisorctl restart streamlit",RELOAD_INTERVAL="60"
autostart=true
autorestart=true
```

2. **Create entrypoint script** (`entrypoints/qa.sh`):
```bash
#!/bin/bash
set -e

echo "[$(date -Iseconds)] QA Streamlit container starting..."
/app/docker/scripts/update-repo.sh

echo "[$(date -Iseconds)] Installing dcpy[all-apps]..."
cd /app/repos/data-engineering
uv pip install --system -e ".[all-apps]"

mkdir -p /var/log/supervisor
exec supervisord -c /app/docker/supervisord/qa.conf
```

3. **Make executable**: `chmod +x entrypoints/qa.sh`

4. **Deploy with QA entrypoint**: `--command "/app/docker/entrypoints/qa.sh"`

## Monitoring and Debugging

**View logs:**
```bash
az containerapp logs show --name dagster --resource-group <rg> --follow
```

**Check reload loop:**
- Look for `[timestamp] Updating /app/repos/data-engineering to latest main...`
- Should appear every 60 seconds

**Manual git update:**
```bash
# Exec into container
az containerapp exec --name dagster --resource-group <rg>

# Manually run update
/app/docker/scripts/update-repo.sh

# Restart app processes
supervisorctl restart all
```

## Notes

- The single-container Dagster setup runs webserver + daemon + job execution together
- For production scale, you can later split into separate containers:
  - Dagster webserver (UI)
  - Dagster daemon (schedules/sensors)
  - Run workers (separate worker pools)
- Git repo is public, no auth needed
- Code updates automatically every minute with graceful app restarts
