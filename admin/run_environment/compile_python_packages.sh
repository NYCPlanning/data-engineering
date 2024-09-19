#!/bin/bash
#
# Dev script to compile python packages from a requirements.in file to a requirements.txt file.
set -e

# Update and install packages used to compile requirements
echo -e "🛠 upgrading python package management tools"
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade pip-tools wheel

# Set GDAL verion
# sed behaves differently in linux and macos
case "$OSTYPE" in
  "darwin"*|"bsd"*)
    echo "Using BSD sed style"
    sed -i "" -e "s/GDAL==.*$/GDAL==$(gdal-config --version)/g" requirements.in
    ;; 
  *)
    echo "Using GNU sed style"
    sed -i -e "s/GDAL==.*$/GDAL==$(gdal-config --version)/g" requirements.in
    ;;
esac

# Compile requirements
echo -e "🛠 compiling from requirements.in"
CUSTOM_COMPILE_COMMAND=$0 python3 -m piptools compile --upgrade --output-file=requirements.txt requirements.in
echo -e "✅ done compiling requirements.txt"

sed -e 's/\[[^][]*\]//g' requirements.txt > constraints.txt
