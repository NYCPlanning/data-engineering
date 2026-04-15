#!/bin/sh
# Generate .htpasswd and nginx config from templates, then start nginx

set -e

# Install htpasswd if not available
if ! command -v htpasswd &> /dev/null; then
    echo "Installing apache2-utils for htpasswd..."
    apk add --no-cache apache2-utils
fi

# Generate htpasswd file
if [ -n "$NGINX_AUTH_USER" ] && [ -n "$NGINX_AUTH_PASSWORD" ]; then
    echo "Generating .htpasswd for user: $NGINX_AUTH_USER"
    htpasswd -cb /etc/nginx/.htpasswd "$NGINX_AUTH_USER" "$NGINX_AUTH_PASSWORD"
else
    echo "Warning: NGINX_AUTH_USER or NGINX_AUTH_PASSWORD not set, using defaults"
    htpasswd -cb /etc/nginx/.htpasswd "detools" "changeme"
fi

# Generate nginx config from template
echo "Generating nginx configuration..."

# Select and generate config based on NGINX_DOMAIN
if [ -n "$NGINX_DOMAIN" ]; then
    echo "Using HTTPS configuration for domain: ${NGINX_DOMAIN}"
    export NGINX_DOMAIN
    envsubst '${NGINX_DOMAIN}' \
        < /etc/nginx/templates/https.conf.template \
        > /etc/nginx/conf.d/default.conf
else
    echo "Using HTTP-only configuration"
    NGINX_SERVER_NAME="${NGINX_SERVER_NAME:-_}"
    export NGINX_SERVER_NAME
    envsubst '${NGINX_SERVER_NAME}' \
        < /etc/nginx/templates/http.conf.template \
        > /etc/nginx/conf.d/default.conf
fi

echo "Configuration generated successfully"

# Start nginx
exec nginx -g 'daemon off;'
