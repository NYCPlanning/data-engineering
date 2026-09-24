FROM nycplanning/build-geosupport:latest

WORKDIR /app

# dcpy-app-qa's full closure (its own deps like streamlit/folium, plus everything
# dcpy-lifecycle needs transitively) resolved from the workspace uv.lock - see
# apps/qa/bash/docker_build_and_publish.sh. Already satisfied by build-geosupport
# for the dcpy-lifecycle portion; this just adds/pins the app's own extras.
COPY dcpy_requirements.txt .
RUN uv pip install --system -r dcpy_requirements.txt


EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# The entrypoint (editable-install dcpy from the mount + run streamlit on the mounted src)
# is set in apps/docker-compose.yml. Running this image without the repo mount is not supported.
