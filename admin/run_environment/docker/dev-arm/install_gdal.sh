#!/bin/bash

source config.sh
set -euo pipefail

gdal_version=3.11.3
gdal_short_version="$(echo "$gdal_version" | tr -d ".")"

REQUIREMENTS="wget lsb-release"


apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y -V install $REQUIREMENTS \

function install_gdal {
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    apt-get update

    apt-get install -y -V libproj-dev libgeos-dev libarrow-dev libparquet-dev cmake

    echo "installing gdal from source" && (
        cd ~
        wget download.osgeo.org/gdal/${gdal_version}/gdal${gdal_short_version}.zip
        unzip gdal${gdal_short_version}.zip
        cd gdal-${gdal_version}
        mkdir build
        cd build
        cmake -DPROJ_INCLUDE_DIR=/usr/include ..
        cmake --build .
        cmake --build . --target install

        cd ~
        rm -rf gdal-${gdal_version}
        rm gdal${gdal_short_version}.zip
    )
    echo "gdal installed successfully"
}
install_gdal
