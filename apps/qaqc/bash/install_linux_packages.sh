#!/bin/bash
#
# Installs linux packages required for building and testing.
set -e

apt-get update
apt-get --assume-yes install --no-install-recommends postgresql-client gdal-bin git-core openssh-client bash-completion
