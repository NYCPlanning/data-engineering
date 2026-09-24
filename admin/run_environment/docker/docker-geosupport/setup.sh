#!/bin/bash

source config.sh
set -e

apt update && apt install -y $GEOSUPPORT_APT_PACKAGES

install_geosupport

# Standalone image, not part of the uv workspace - no shared lock to pin against.
pip install -r requirements.txt
