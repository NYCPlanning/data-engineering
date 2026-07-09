# Unified Dockerfile for all data-engineering apps
# Built FROM nycplanning/build-geosupport:latest
# Pushed to ACR as de-apps:latest
# Different apps use different entrypoints (set at runtime)

FROM nycplanning/build-geosupport:latest

WORKDIR /app

# Install git for cloning the repo
RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

# Copy shared docker scripts and configs
COPY docker/scripts /app/docker/scripts
COPY docker/supervisord /app/docker/supervisord
COPY docker/entrypoints /app/docker/entrypoints

# Ensure scripts are executable
RUN chmod +x /app/docker/scripts/*.sh /app/docker/entrypoints/*.sh

# Set up environment variables with defaults
# These can be overridden at runtime
ENV REPO_URL=https://github.com/NYCPlanning/data-engineering.git
ENV REPO_BRANCH=main
ENV REPO_DIR=/app/repos/data-engineering
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Create necessary directories
RUN mkdir -p ${DAGSTER_HOME} /var/log/supervisor

# Expose common ports (can be remapped in Azure Container Apps)
EXPOSE 3000 8501 8080

# Default entrypoint (override for different apps)
# For dagster: /app/docker/entrypoints/dagster.sh
# For qa: /app/docker/entrypoints/qa.sh (to be created)
# For notebook: /app/docker/entrypoints/notebook.sh (to be created)
ENTRYPOINT ["/bin/bash"]
CMD ["-c", "echo 'Please specify an entrypoint for this container'"]
