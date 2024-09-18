ARG base_tag=latest
FROM nycplanning/base:${base_tag}

ARG RELEASE
ARG MAJOR
ARG MINOR
ARG PATCH

# Geosupport required env variables
ENV GEOFILES=/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/fls/
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/lib/

COPY . .

RUN ./setup.sh
