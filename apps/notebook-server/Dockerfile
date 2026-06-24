# syntax=docker/dockerfile:1

FROM nycplanning/build-base:latest

WORKDIR /app

# Only the notebook server's own deps (marimo, uvicorn); dcpy's deps come from build-base.
COPY --link admin/run_environment/constraints.txt admin/run_environment/
COPY --link apps/notebook-server/requirements.txt apps/notebook-server/
RUN uv pip install --system -r apps/notebook-server/requirements.txt -c admin/run_environment/constraints.txt

EXPOSE 8080

HEALTHCHECK CMD curl --fail http://localhost:8080/ || exit 1

# The entrypoint (editable-install dcpy from the mount + run the server) is set in
# docker-compose.yml. Running this image without the repo mount is not supported.
