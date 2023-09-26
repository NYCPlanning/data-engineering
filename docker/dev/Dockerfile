ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim

ARG RELEASE
ARG MAJOR
ARG MINOR
ARG PATCH

# Geosupport required env variables
ENV GEOFILES=/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/fls/
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/lib/

# GDAL for python required env variables
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

COPY . .

RUN ./setup.sh
