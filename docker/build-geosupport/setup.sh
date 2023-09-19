#!/bin/bash
source config.sh
set -e

apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install $BUILD_APT_PACKAGES

install_geosupport
install_yq
install_mc

python3 -m pip install -r requirements.txt -c constraints.txt
