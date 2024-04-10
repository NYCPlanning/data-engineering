#!/bin/bash
#
# Dev script to compile python packages from a requirements.in file to a requirements.txt file.
set -e

# Update and install packages used to compile requirements
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade pip-tools wheel

# Delete exisitng requirements file to ensure full dependency resolution
rm --force requirements.txt

# Compile requirements
CUSTOM_COMPILE_COMMAND="./utils/dev_python_packages.sh" python3 -m piptools compile --output-file=requirements.txt requirements.in
