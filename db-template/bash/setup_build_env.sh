#!/bin/bash
#
# Sets up environment for running build scripts in either local devcontainer or github action.
set -e

sudo apt-get update

# Install postgresql client to use psql (postgresql alias)
sudo apt-get --assume-yes install --no-install-recommends postgresql-client

# Install minio for S3 file I/O
curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv ./mc /usr/bin

# Install python packages
source bash/install_python_packages.sh
