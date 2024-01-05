ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

COPY . .

RUN ./setup.sh
