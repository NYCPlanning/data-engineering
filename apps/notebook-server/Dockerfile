# syntax=docker/dockerfile:1

FROM nycplanning/build-geosupport:latest

WORKDIR /app

# dcpy-app-notebook-server's full closure (its own deps like marimo/uvicorn, plus everything
# dcpy-lifecycle needs transitively) resolved from the workspace uv.lock - see
# apps/bash/smoke_test.sh. Already satisfied by build-geosupport for the dcpy-lifecycle
# portion; this just adds/pins the app's own extras.
COPY --link apps/notebook-server/dcpy_requirements.txt apps/notebook-server/
RUN uv pip install --system -r apps/notebook-server/dcpy_requirements.txt

EXPOSE 8080

HEALTHCHECK CMD curl --fail http://localhost:8080/ || exit 1

# The entrypoint (editable-install dcpy from the mount + run the server) is set in
# docker-compose.yml. Running this image without the repo mount is not supported.
