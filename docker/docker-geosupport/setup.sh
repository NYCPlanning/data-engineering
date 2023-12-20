#!/bin/bash

source config.sh
set -e

apt update && apt install -y $GEOSUPPORT_APT_PACKAGES

install_geosupport

pip install -r requirements.txt -c constraints.txt
