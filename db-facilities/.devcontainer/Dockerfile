# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.163.1/containers/python-3/.devcontainer/base.Dockerfile

# [Choice] Python version: 3, 3.9, 3.8, 3.7, 3.6
ARG VARIANT="3.10"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

# [Option] Install Node.js
ARG INSTALL_NODE="true"
ARG NODE_VERSION="lts/*"
RUN if [ "${INSTALL_NODE}" = "true" ]; then su vscode -c "umask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends postgresql-client gdal-bin

# Install Geosupport
ARG RELEASE=23a
ARG MAJOR=23
ARG MINOR=1
ARG PATCH=0

WORKDIR /geosupport
RUN FILE_NAME=linux_geo${RELEASE}_${MAJOR}_${MINOR}.zip\
    && echo $FILE_NAME\
    && curl -O https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/$FILE_NAME\
    && unzip *.zip\
    && rm *.zip

ENV GEOFILES=/geosupport/version-${RELEASE}_${MAJOR}.${MINOR}/fls/
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/geosupport/version-${RELEASE}_${MAJOR}.${MINOR}/lib/
WORKDIR /

#Install ipython
# RUN pip install ipython

#Install minio
RUN curl -O https://dl.min.io/client/mc/release/linux-amd64/mc\
    && chmod +x mc\
    && sudo mv ./mc /usr/bin


# Install Poetry
USER vscode
RUN curl -sSL https://install.python-poetry.org | python3 -
