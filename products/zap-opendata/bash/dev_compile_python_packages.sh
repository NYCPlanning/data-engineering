#!/bin/bash
#
# Dev script to compile python packages from a requirements.in file to a requirements.txt file.
set -e
RELATIVE_SCRIPTPATH=$0

# Update and install packages used to compile requirements
apt-get -yq install python3-venv
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade pip-tools wheel validate-pyproject

# Validate pyproject.toml
validate-pyproject pyproject.toml

# Delete existing requirements file to ensure full dependency resolution
rm -rf requirements.txt

# Compile requirements
CUSTOM_COMPILE_COMMAND="${RELATIVE_SCRIPTPATH}" python3 -m piptools compile --output-file=requirements.txt pyproject.toml
