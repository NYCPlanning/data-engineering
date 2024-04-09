#!/bin/bash
source config.sh
set -e

install_geosupport

python3 -m pip install -r requirements.txt -c constraints.txt
python3 -m pip install -r dcpy_requirements.txt -c constraints.txt
