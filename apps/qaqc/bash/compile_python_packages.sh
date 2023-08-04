#!/bin/bash
#
# Dev script to compile python packages from a requirements.in file to a requirements.txt file.
set -e

RELATIVE_SCRIPTPATH=$(realpath --relative-to="${PWD}" "$0")

# Update and install packages used to compile requirements
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade pip-tools wheel

# Delete exisitng requirements file to ensure full dependency resolution
rm --force requirements.txt

# Compile requirements
CUSTOM_COMPILE_COMMAND="./${RELATIVE_SCRIPTPATH}" python3 -m piptools compile --output-file=requirements.txt requirements.in