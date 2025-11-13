#!/bin/bash
source config.sh
set -e

apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install $DEV_APT_PACKAGES

install_geosupport

uv pip install --system -r requirements.txt
