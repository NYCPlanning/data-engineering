#!/bin/bash

source config.sh
set -e

apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y -V install $COMMON_APT_PACKAGES \

install_yq
install_mc
install_gdal
