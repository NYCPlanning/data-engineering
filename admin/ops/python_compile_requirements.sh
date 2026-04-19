#!/bin/bash

# Dev script to compile python packages from a requirements.in file to a requirements.txt file.
# Can be run from anywhere, but assumes that relative to this script, there is "../run_environment"
#   folder with python requirements present
#
# Usage:
#   admin/ops/python_compile_requirements.sh [OPTIONS]
#
# Options:
#   --set-gdal    Set the GDAL Python package version in requirements.in to match
#                 the locally installed GDAL binary version
#   --upgrade     Upgrade all package versions when compiling requirements.txt

set -e

ops_dir=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
path=$ops_dir/../run_environment

# Set GDAL python version to installed binary gdal version, if specified
# sed behaves differently in linux and macos
if [[ $* == *--set-gdal* ]]; then
  case "$OSTYPE" in
    "darwin"*|"bsd"*)
      echo "Using BSD sed style"
      sed -i "" -e "s/GDAL==.*$/GDAL==$(gdal-config --version)/g" $path/requirements.in
      ;; 
    *)
      echo "Using GNU sed style"
      sed -i -e "s/GDAL==.*$/GDAL==$(gdal-config --version)/g" $path/requirements.in
      ;;
  esac
fi

# if specified, upgrade versions in requirements.txt
if [[ $* == *--upgrade* ]]; then
  upgrade="--upgrade"
else
  upgrade=""
fi

cd $path

echo -e "🛠 compiling from requirements.in"
uv pip compile requirements.in $upgrade --output-file requirements.txt --no-strip-extras --custom-compile-command $0
echo -e "✅ done compiling requirements.txt"

sed -e 's/\[[^][]*\]//g' requirements.txt > constraints.txt

cd $ops_dir/../
