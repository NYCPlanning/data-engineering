#!/bin/bash
# Initialize SSL certificates with Let's Encrypt
# Run with: op run --env-file=env.1pw -- ./scripts/init-ssl.sh [domain] [email]

set -e

DOMAIN=${1:-${NGINX_DOMAIN:-de-qaqc.nycplanningdigital.com}}
EMAIL=${2:-ITD-GDE-DE_DL@planning.nyc.gov}

echo "Initializing SSL certificate for $DOMAIN..."
echo "Email: $EMAIL"

# Start nginx and certbot services
docker compose up -d nginx certbot

# Wait for nginx to be ready
sleep 5

# Request certificate
docker compose exec certbot certbot certonly \
    --webroot \
    --webroot-path=/var/www/certbot \
    --email "$EMAIL" \
    --agree-tos \
    --no-eff-email \
    --force-renewal \
    -d "$DOMAIN"

# Reload nginx to pick up certificates
docker compose exec nginx nginx -s reload

echo "SSL certificate initialized successfully!"
echo "Certificate will auto-renew via the certbot service"
