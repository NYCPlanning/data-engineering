#!/bin/bash
#
# Installs python packages required for building and testing pipeline.
set -e
path_to_requirements="${1:-"."}"

# Install python requirements
python3 -m pip install --upgrade pip
python3 -m pip install --requirement ${path_to_requirements}/requirements.txt
