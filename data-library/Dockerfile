FROM python:3.11-slim

COPY . /library

WORKDIR /library

RUN ./library.sh setup

WORKDIR /

RUN rm -rf library

# GDAL for python required env variables
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
