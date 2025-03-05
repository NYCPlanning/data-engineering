#!/bin/bash

# Dev script to compile python packages from a requirements.in file to a requirements.txt file.
# Can be run from anywhere, but assumes that relative to this script, there is "../run_environment"
#   folder with python requirements present
# No arguments

set -e

ops_dir=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
path=$ops_dir/../run_environment

cd $path

if [ "$1" == "--no-upgrade" ]; then
  echo -e "🛠 compiling from requirements.in"
  uv pip compile requirements.in --output-file requirements.txt --custom-compile-command $0
  echo -e "✅ done compiling requirements.txt"

else
  # Set GDAL verion
  # sed behaves differently in linux and macos
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
  
  echo -e "🛠 compiling from requirements.in"
  uv pip compile requirements.in --upgrade --output-file requirements.txt --custom-compile-command $0
  echo -e "✅ done compiling requirements.txt"
fi

sed -e 's/\[[^][]*\]//g' requirements.txt > constraints.txt

cd $ops_dir/../
