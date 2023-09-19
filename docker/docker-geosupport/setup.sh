#!/bin/bash

source config.sh
set -e

apt update && apt install -y curl git unzip zip build-essential

install_geosupport

pip install -r requirements.txt -c constraints.txt
