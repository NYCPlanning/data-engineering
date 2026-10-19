#!/bin/bash

source config.sh
set -e

gdal_version=3.13.1
gdal_short_version="$(echo "$gdal_version" | tr -d ".")"
# must be >= the build engine's server version: pg_dump refuses to dump a newer server
postgres_client_version=18
COMMON_APT_PACKAGES="curl zip unzip git wget ca-certificates lsb-release build-essential sudo jq locales pandoc weasyprint"

apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y -V install $COMMON_APT_PACKAGES \


function install_yq {
    wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq \
        && chmod +x /usr/bin/yq
}


function install_gdal {
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    sudo apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    sudo apt-get update

    sudo apt-get install -y -V libproj-dev libgeos-dev libarrow-dev libparquet-dev cmake

    echo "installing gdal from source" && (
        cd ~
        wget download.osgeo.org/gdal/${gdal_version}/gdal${gdal_short_version}.zip
        unzip gdal${gdal_short_version}.zip
        cd gdal-${gdal_version}
        mkdir build
        cd build
        sudo cmake -DPROJ_INCLUDE_DIR=/usr/include ..
        sudo cmake --build .
        sudo cmake --build . --target install

        cd ~
        rm -rf gdal-${gdal_version}
        rm gdal${gdal_short_version}.zip
    )
    echo "gdal installed successfully"
}


# Debian's own postgresql-client trails the newest major version, so install from the PGDG repo
function install_postgres_client {
    install -d /usr/share/postgresql-common/pgdg
    curl --fail -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release --codename --short)-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list
    apt-get update
    apt-get install -y -V postgresql-client-${postgres_client_version} libpq-dev
}

install_yq
install_postgres_client
install_gdal
