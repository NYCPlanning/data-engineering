FROM python:3.11-slim

ARG RELEASE
ARG MAJOR
ARG MINOR
ARG PATCH

ENV GEOFILES=/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/fls/
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/lib/

RUN apt update && apt install -y curl git unzip zip build-essential

WORKDIR /geocode

COPY . . 

RUN ./setup.sh

WORKDIR /
