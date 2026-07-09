#!/bin/sh
# Generate nginx config from templates, then start nginx
# Azure Container Apps version - no password auth

set -e

echo "Generating nginx configuration..."

# Set default backend URLs for Container Apps internal DNS
# These can be overridden via environment variables
BACKEND_DAGSTER_URL="${BACKEND_DAGSTER_URL:-http://dagster:3000}"
BACKEND_QA_URL="${BACKEND_QA_URL:-http://qa-streamlit:8501/qaqc}"

export BACKEND_DAGSTER_URL
export BACKEND_QA_URL

echo "Backend routes configured:"
echo "  Dagster: ${BACKEND_DAGSTER_URL}"
echo "  QA: ${BACKEND_QA_URL}"

# Select and generate config based on NGINX_DOMAIN
if [ -n "$NGINX_DOMAIN" ]; then
    echo "Using HTTPS configuration for domain: ${NGINX_DOMAIN}"

    # Set default cert paths if not provided (Azure Container Apps typically mounts to /mnt/certificates)
    NGINX_SSL_CERT="${NGINX_SSL_CERT:-/etc/nginx/ssl/cert.pem}"
    NGINX_SSL_KEY="${NGINX_SSL_KEY:-/etc/nginx/ssl/key.pem}"

    export NGINX_DOMAIN
    export NGINX_SSL_CERT
    export NGINX_SSL_KEY

    echo "SSL Certificate: ${NGINX_SSL_CERT}"
    echo "SSL Key: ${NGINX_SSL_KEY}"

    envsubst '${NGINX_DOMAIN} ${NGINX_SSL_CERT} ${NGINX_SSL_KEY} ${BACKEND_DAGSTER_URL} ${BACKEND_QA_URL}' \
        < /etc/nginx/templates/https.conf.template \
        > /etc/nginx/conf.d/default.conf
else
    echo "Using HTTP-only configuration"
    NGINX_SERVER_NAME="${NGINX_SERVER_NAME:-_}"
    export NGINX_SERVER_NAME
    envsubst '${NGINX_SERVER_NAME} ${BACKEND_DAGSTER_URL} ${BACKEND_QA_URL}' \
        < /etc/nginx/templates/http.conf.template \
        > /etc/nginx/conf.d/default.conf
fi

echo "Configuration generated successfully"

# Start nginx
exec nginx -g 'daemon off;'
