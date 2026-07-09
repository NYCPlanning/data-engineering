# Nginx Reverse Proxy for Azure Container Apps

Custom nginx reverse proxy for Azure Container Apps environment. Based on nginx:alpine with no password authentication and configurable SSL certificate paths.

## Key Differences from apps/nginx/

- **No password authentication**: Removed `auth_basic` directives
- **Configurable SSL cert paths**: Cert and key paths set via environment variables
- **Azure-optimized**: Designed for Azure Container Apps internal networking

## Configuration

### Environment Variables

**Backend Service URLs:**
- `BACKEND_DAGSTER_URL` (optional): Dagster backend URL (default: `http://dagster:3000`)
- `BACKEND_QA_URL` (optional): QA Streamlit backend URL (default: `http://qa-streamlit:8501/qaqc`)

**HTTP Mode (No SSL):**
- `NGINX_SERVER_NAME` (optional): Server name, defaults to `_` (catch-all)

**HTTPS Mode (With SSL):**
- `NGINX_DOMAIN` (required): Domain name for the SSL certificate
- `NGINX_SSL_CERT` (optional): Path to SSL certificate file (default: `/etc/nginx/ssl/cert.pem`)
- `NGINX_SSL_KEY` (optional): Path to SSL private key file (default: `/etc/nginx/ssl/key.pem`)

### Routing and Internal DNS

The nginx config proxies to backend services:

- `/` → Static landing page
- `/qaqc` → `${BACKEND_QA_URL}` (default: `http://qa-streamlit:8501/qaqc`)
- `/dag` → `${BACKEND_DAGSTER_URL}` (default: `http://dagster:3000`)

**How Azure Container Apps Internal DNS Works:**

When you deploy multiple container apps in the same Container Apps Environment:
1. Each app gets an internal DNS name matching its app name
2. Apps can reach each other using `http://<app-name>:<port>`
3. No additional networking configuration needed

**Example:**
```bash
# Deploy dagster app
az containerapp create --name dagster --target-port 3000 ...

# Deploy qa-streamlit app
az containerapp create --name qa-streamlit --target-port 8501 ...

# Deploy nginx with default backend URLs (will use internal DNS)
az containerapp create --name nginx ...
# nginx can now proxy to http://dagster:3000 and http://qa-streamlit:8501
```

**Custom Backend URLs:**
If your apps have different names or you want to use external URLs:
```bash
az containerapp create \
  --name nginx \
  --env-vars \
    BACKEND_DAGSTER_URL=http://my-dagster-app:3000 \
    BACKEND_QA_URL=http://my-qa-app:8501/qaqc \
  ...
```

## Deployment

### Build and Push to ACR

Use the GitHub Actions workflow:

1. Go to Actions → "Azure ACR - Build and Publish App Images"
2. Check "Build and push 'nginx-azure' image"
3. Run workflow

Image will be pushed to: `${AZURE_ACR_LOGIN_SERVER}/nginx-azure:latest`

### Deploy to Azure Container Apps

**HTTP-only (no SSL cert yet):**

```bash
az containerapp create \
  --name nginx \
  --resource-group <your-rg> \
  --environment <your-env> \
  --image <your-acr>.azurecr.io/nginx-azure:latest \
  --target-port 80 \
  --ingress external \
  --cpu 0.5 \
  --memory 1Gi
```

**HTTPS with mounted certificate:**

```bash
# First, add the certificate to your Container Apps Environment
az containerapp env certificate upload \
  --name <your-env> \
  --resource-group <your-rg> \
  --certificate-file /path/to/cert.pem \
  --certificate-key-file /path/to/key.pem \
  --certificate-name my-cert

# Then create the app with cert mounted
az containerapp create \
  --name nginx \
  --resource-group <your-rg> \
  --environment <your-env> \
  --image <your-acr>.azurecr.io/nginx-azure:latest \
  --target-port 443 \
  --ingress external \
  --env-vars \
    NGINX_DOMAIN=your-domain.com \
    NGINX_SSL_CERT=/mnt/certificates/cert.pem \
    NGINX_SSL_KEY=/mnt/certificates/key.pem \
  --cpu 0.5 \
  --memory 1Gi
```

Note: Azure Container Apps certificate mounting paths may vary. Adjust `NGINX_SSL_CERT` and `NGINX_SSL_KEY` based on where Azure mounts your certificates.

## Local Testing

```bash
# Build
docker build -t nginx-azure:latest apps/nginx_azure/

# Run HTTP-only mode
docker run -p 80:80 nginx-azure:latest

# Run HTTPS mode (with local certs)
docker run -p 443:443 \
  -v /path/to/cert.pem:/etc/nginx/ssl/cert.pem:ro \
  -v /path/to/key.pem:/etc/nginx/ssl/key.pem:ro \
  -e NGINX_DOMAIN=localhost \
  nginx-azure:latest
```

## SSL Certificate Sources

Azure Container Apps supports multiple certificate sources:

1. **Managed certificates**: Azure-managed certs for custom domains
2. **Uploaded certificates**: Upload your own cert via `az containerapp env certificate upload`
3. **Key Vault certificates**: Reference certs stored in Azure Key Vault

Once mounted, set the environment variables to point to the mounted paths.

**Note:** This nginx config does not use certbot or Let's Encrypt ACME challenges. SSL certificates should be managed at the Azure platform level or manually uploaded.

## Security Notes

- No password authentication - ensure network-level security via Azure private networking or VNET integration
- SSL/TLS configuration uses modern protocols (TLSv1.2+) and strong ciphers
- Security headers enabled in HTTPS mode: HSTS, X-Frame-Options, X-Content-Type-Options, X-XSS-Protection
- For production, enable HTTPS and configure proper certificate management

## Troubleshooting

**"Cannot find certificate" error:**
- Check that `NGINX_SSL_CERT` and `NGINX_SSL_KEY` point to the correct mounted paths
- Verify certificate is mounted in the container using `az containerapp exec`

**Backends not reachable:**
- Ensure backend services are in the same Container Apps Environment (required for internal DNS)
- Check service names: Default expects apps named `dagster` and `qa-streamlit`
- If using different names, override with `BACKEND_DAGSTER_URL` and `BACKEND_QA_URL` env vars
- Verify backend apps are running: `az containerapp list --environment <env-name>`

**HTTP works but HTTPS fails:**
- Normal if cert is not yet mounted - deploy in HTTP mode first
- Once cert is mounted, set `NGINX_DOMAIN` env var to enable HTTPS config
