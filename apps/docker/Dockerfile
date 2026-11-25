# Shared image for the data-engineering apps (qa, dagster, notebook-server), pushed to ACR as
# de-apps:latest. The repo is baked in at /app/data-engineering with every app's dependencies
# installed, so each app is just a startup script: apps/docker/entrypoints/<app>.sh.
#
# Build from the repo root:
#   docker build -f apps/docker/Dockerfile -t de-apps .
# What's copied in is allowlisted by Dockerfile.dockerignore (next to this file).

# Resolve third-party requirements in a throwaway stage, so the install layer below is only
# rebuilt when uv.lock actually changes - not on every code change.
FROM nycplanning/build-geosupport:latest AS requirements
WORKDIR /src
COPY . .
RUN uv export --frozen --no-dev --no-hashes --no-emit-workspace \
        --package dcpy-app-qa \
        --package dcpy-app-dagster \
        --package dcpy-app-notebook-server \
        -o /requirements.txt


FROM nycplanning/build-geosupport:latest

COPY --from=requirements /requirements.txt /tmp/requirements.txt
RUN uv pip install --system -r /tmp/requirements.txt

WORKDIR /app/data-engineering
COPY . .

# Editable, so dcpy resolves repo-relative paths (product-metadata/, products/) from here.
RUN uv pip install --system --no-deps $(for d in python/*/; do printf -- '-e %s ' "$d"; done) && \
    chmod +x apps/docker/entrypoints/*.sh

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p ${DAGSTER_HOME}

EXPOSE 3000 4000 8501

CMD ["echo", "Specify an app entrypoint, e.g. apps/docker/entrypoints/qa.sh"]
