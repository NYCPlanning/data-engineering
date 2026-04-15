# Docker Compose Apps

## Local Development

Requires environment variables from root `example.env`. 

```bash
cd apps
./scripts/local-start.sh
```

Access at:
- http://localhost:8080/qaqc (QA app)
- http://localhost:8080/dag (Dagster)
- Login: username/password from your environment

## Production Deployment

### Prerequisites

Install 1Password CLI on droplet

# Set service account token
export OP_SERVICE_ACCOUNT_TOKEN="your-token-here"
# Add to ~/.bashrc or /etc/environment for persistence
```

### Initial Setup (IP-only mode)

```bash
git clone https://github.com/NYCPlanning/data-engineering-de1.git
cd data-engineering-de1/apps
op run --env-file=env.1pw -- ./scripts/start.sh
```

Access at: `http://YOUR_DROPLET_IP/qaqc` and `/dag`

### Adding SSL (when domain is configured)

```bash
# Initialize SSL certificate
op run --env-file=.env -- ./scripts/init-ssl.sh

# Update .env to set NGINX_DOMAIN
sed -i 's/NGINX_DOMAIN=$/NGINX_DOMAIN=de-qaqc.nycplanningdigital.com/' .env

# Restart with SSL enabled
op run --env-file=.env -- ./scripts/start.sh
```

Access at: `https://de-qaqc.nycplanningdigital.com/qaqc` and `/dag`

### Deploy Updates

```bash
op run --env-file=.env -- ./scripts/deploy.sh
```

Or use GitHub Actions: `.github/workflows/docker_deploy.yml`
