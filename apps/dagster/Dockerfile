FROM nycplanning/build-base:latest

WORKDIR /app/dagster

# Copy only requirements to install dependencies
COPY requirements.txt /tmp/dagster-requirements.txt

# Install dagster dependencies
RUN uv pip install --system -r /tmp/dagster-requirements.txt

# Install dcpy from the data-engineering repo (will be available via mount at runtime)
# For build time, we need to ensure dcpy is available when containers run
# Note: The actual dcpy code will be mounted at /app/repos/data-engineering at runtime
RUN mkdir -p /app/repos/data-engineering

# Set up dagster home
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p ${DAGSTER_HOME}

# Expose dagster webserver port
EXPOSE 3000

# Install dcpy at runtime from mounted volume
# Note: dagster code and dcpy will be mounted at runtime via volumes
CMD sh -c "uv pip install --system -e /app/repos/data-engineering && dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml --path-prefix /dag"
