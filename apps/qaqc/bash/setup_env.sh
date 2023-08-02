#!/bin/bash
#
# Sets up environment for running build scripts in either local devcontainer or github action.
set -e

./bash/install_linux_packages.sh
./bash/install_python_packages.sh