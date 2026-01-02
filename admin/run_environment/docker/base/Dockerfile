ARG PYTHON_VERSION=3.14
FROM python:${PYTHON_VERSION}-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV LD_LIBRARY_PATH=/usr/local/lib

COPY . .

RUN ./setup.sh
