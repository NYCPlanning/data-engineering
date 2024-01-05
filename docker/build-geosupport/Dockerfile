FROM nycplanning/base:latest

ARG RELEASE
ARG MAJOR
ARG MINOR
ARG PATCH

ENV GEOFILES=/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/fls/
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/lib/

COPY . .

RUN ./setup.sh
