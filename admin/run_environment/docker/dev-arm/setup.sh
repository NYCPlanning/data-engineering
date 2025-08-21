#!/bin/bash

source config.sh
set -euo pipefail

COMMON_APT_PACKAGES="curl vim zip unzip git wget ca-certificates lsb-release build-essential sudo postgresql-client libpq-dev jq locales pandoc weasyprint"

apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y -V install $COMMON_APT_PACKAGES \

# Install Direnv
curl -sfL https://direnv.net/install.sh | bash
echo 'eval "$(direnv hook bash)"' > ~/.bashrc
source ~/.bashrc

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install DuckDB
curl https://install.duckdb.org | sh
