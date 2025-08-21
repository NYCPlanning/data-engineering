FROM dcp-gdal-python-arm:3.11.3

WORKDIR /de
COPY setup.sh requirements.txt pyproject.toml .

RUN ./setup.sh; uv pip install -e . --system; uv pip install -r requirements.txt --system

SHELL ["/bin/bash", "-c"]
