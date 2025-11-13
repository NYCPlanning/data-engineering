#!/bin/bash
source config.sh
set -e

install_geosupport

uv pip install --system -r requirements.txt -c constraints.txt
uv pip install --system -r dcpy_requirements.txt -c constraints.txt
