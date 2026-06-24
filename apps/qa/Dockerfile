FROM nycplanning/build-base:latest

WORKDIR /app

# Only the QA app's own deps (streamlit, folium, …); dcpy's deps come from build-base.
COPY constraints.txt .
COPY requirements.txt .
RUN uv pip install --system -r requirements.txt -c constraints.txt


EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

# The entrypoint (editable-install dcpy from the mount + run streamlit on the mounted src)
# is set in apps/docker-compose.yml. Running this image without the repo mount is not supported.
