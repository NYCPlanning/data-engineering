FROM python:3.10
# FROM python@sha256:c5f60863db103c951595f110def9244c1e09efe9e8d072cfac3da39310bc8cc8

# install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends zip unzip curl postgresql-client build-essential jq

# Install Geosupport
ARG RELEASE=23a
ARG MAJOR=23
ARG MINOR=1
ARG PATCH=0

WORKDIR /geosupport
RUN FILE_NAME=linux_geo${RELEASE}_${MAJOR}_${MINOR}.zip\
    && echo $FILE_NAME\
    && curl -O https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/$FILE_NAME\
    && unzip -qq *.zip\
    && rm *.zip

ENV GEOFILES=/geosupport/version-${RELEASE}_${MAJOR}.${MINOR}/fls/
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/geosupport/version-${RELEASE}_${MAJOR}.${MINOR}/lib/

# Copy files and poetry install
WORKDIR /src
COPY . .

RUN curl -sSL https://install.python-poetry.org | python3 -

RUN export PATH="/root/.local/bin:$PATH" &&\
    poetry config virtualenvs.create false --local &&\
    poetry install --no-dev
