ARG PYTHON_VERSION
FROM python:3.11-slim

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV LD_LIBRARY_PATH=/usr/local/lib

COPY . .

RUN ./setup.sh
