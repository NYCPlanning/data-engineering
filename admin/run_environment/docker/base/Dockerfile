ARG PYTHON_VERSION=3.13
FROM python:${PYTHON_VERSION}-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV LD_LIBRARY_PATH=/usr/local/lib

COPY . .

RUN ./setup.sh
