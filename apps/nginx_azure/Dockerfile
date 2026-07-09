# Nginx reverse proxy for Azure Container Apps
# No password authentication, SSL cert paths configurable

FROM nginx:alpine

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

# Copy nginx main config
COPY nginx.conf /etc/nginx/nginx.conf

# Copy config templates
COPY conf.d/*.template /etc/nginx/templates/

# Copy landing page
COPY html /usr/share/nginx/html

# Copy entrypoint script
COPY scripts/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Create SSL directory (certs will be mounted at runtime)
RUN mkdir -p /etc/nginx/ssl

# Expose HTTP and HTTPS
EXPOSE 80 443

ENTRYPOINT ["/docker-entrypoint.sh"]
