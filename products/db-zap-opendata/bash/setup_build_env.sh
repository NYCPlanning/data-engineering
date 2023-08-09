#!/bin/bash
set -e

echo "Setting up build environment ..."

# install python packages
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade pip-tools wheel
python3 -m pip install --requirement requirements.txt

echo "Done!"