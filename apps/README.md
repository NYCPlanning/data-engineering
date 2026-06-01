# DE Tools — Docker Compose Apps

A multi-service stack running behind a single nginx reverse proxy, deployed on a DigitalOcean droplet at `de-qaqc.nycplanningdigital.com`. Services can also be run standalone for local development.

## Services

| Service | URL path | Description |
|---|---|---|
| **qa-streamlit** | `/qaqc` | Streamlit QA/QAQC app — product comparison pages for PLUTO, DevDB, FACDB, and others |
| **dagster** | `/dag` | Dagster webserver — asset catalog, run history, and manual job launches |
| **dagster-daemon** | — | Runs scheduled and sensor-triggered Dagster jobs; no UI |
| **nginx** | `/` | Reverse proxy; handles basic auth, SSL termination, and path routing |
| **certbot** | — | Production-only; auto-renews Let's Encrypt certificates every 12 hours |
| **notebook-server** | — | Standalone marimo notebook server (separate `notebook-server/docker-compose.yml`; not wired into the main stack) |

## Architecture

[View architecture diagram](https://htmlpreview.github.io/?https://raw.githubusercontent.com/NYCPlanning/data-engineering/main/docs/de-tools/architecture.html)

## Running locally

### QA app standalone

Runs only the Streamlit QA app without Docker.

```bash
cd apps/qa
cp example.env .env   # fill in values
streamlit run src/index.py
```

Access at: http://localhost:8501

### Dagster standalone

For working on ingest/build pipelines without the full Docker stack. Requires `dagster` and `dcpy` installed in your environment.

```bash
# From repo root, with .venv active:
pip install -e .
cd apps/dagster
# Set required env vars (see apps/example.env for the full list)
dagster dev
```

Access at: http://localhost:3000

### Full Docker Compose stack

Runs all services behind nginx. Useful when testing nginx routing, auth, or multi-service interactions.

```bash
cd apps
cp example.env .env   # fill in values
./scripts/local-start.sh
```

Access at:
- http://localhost:8080/qaqc (QA app)
- http://localhost:8080/dag (Dagster)
- Login: `NGINX_AUTH_USER` / `NGINX_AUTH_PASSWORD` from your `.env`

## Production Deployment

All production commands use the 1Password CLI to inject secrets from `env.1pw`.

### Prerequisites

Install 1Password CLI on the DigitalOcean droplet and set a service account token:

```bash
export OP_SERVICE_ACCOUNT_TOKEN="your-token-here"
# Add to ~/.bashrc or /etc/environment for persistence
```

### Initial Setup (IP-only mode)

```bash
git clone https://github.com/NYCPlanning/data-engineering.git /root/data-engineering
cd /root/data-engineering/apps
op run --env-file=env.1pw -- ./scripts/start.sh
```

Access at: `http://YOUR_DROPLET_IP/qaqc` and `/dag`

### Adding SSL (when domain is configured)

```bash
# Initialize SSL certificate
op run --env-file=env.1pw -- ./scripts/init-ssl.sh

# Set NGINX_DOMAIN in env.1pw, then restart with SSL enabled
op run --env-file=env.1pw -- ./scripts/start.sh
```

Access at: `https://de-qaqc.nycplanningdigital.com/qaqc` and `/dag`

### Deploy Updates

```bash
op run --env-file=env.1pw -- ./scripts/deploy.sh
```

Or use GitHub Actions: `.github/workflows/qa_deploy.yml`
