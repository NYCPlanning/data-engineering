#!/bin/bash
source config.sh
set -e

apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install $DEV_APT_PACKAGES \
    && apt-get -y install --reinstall build-essential # needed to install gdal. I don't know why, it just is

install_geosupport
install_yq
install_mc

python3 -m pip install -r requirements.txt
