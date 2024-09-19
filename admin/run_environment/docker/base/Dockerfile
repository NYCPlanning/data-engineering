ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV LD_LIBRARY_PATH=/usr/local/lib

COPY . .

RUN ./setup.sh
