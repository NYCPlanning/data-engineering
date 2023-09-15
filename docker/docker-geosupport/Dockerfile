FROM python:3.9-slim

ARG RELEASE=21c
ARG MAJOR=21
ARG MINOR=3
ARG PATCH=0

RUN apt update && apt install -y curl git unzip zip build-essential

WORKDIR /geocode

COPY . . 

RUN FILE_NAME=linux_geo${RELEASE}_${MAJOR}_${MINOR}.zip\
    && echo $FILE_NAME\
    && curl -O https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/$FILE_NAME\
    && unzip *.zip\
    && rm *.zip

RUN ./patch.sh

ENV GEOFILES=/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/fls/
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/geocode/version-${RELEASE}_${MAJOR}.${MINOR}/lib/

RUN pip install --upgrade pip \
    && pip install git+https://github.com/NYCPlanning/python-geosupport.git\
    && pip install -r requirements.txt

WORKDIR /
