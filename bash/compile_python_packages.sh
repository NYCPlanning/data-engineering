#!/bin/bash
#
# Dev script to compile python packages from a requirements.in file to a requirements.txt file.
set -e
path_to_requirements="${1:-"."}"
RELATIVE_SCRIPTPATH=$(realpath --relative-to="${PWD}" "$0")

# Update and install packages used to compile requirements
echo -e "🛠 upgrading python package management tools"
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade pip-tools wheel

echo -e "🛠 deleting requirements.txt"
# Delete existing requirements file to ensure full dependency resolution
rm --force ${path_to_requirements}/requirements.txt

# Compile requirements
echo -e "🛠 compiling requirements.txt"
CUSTOM_COMPILE_COMMAND="./${RELATIVE_SCRIPTPATH}" python3 -m piptools compile --output-file=${path_to_requirements}/requirements.txt ${path_to_requirements}/requirements.in
echo -e "✅ done compiling requirements.txt"
