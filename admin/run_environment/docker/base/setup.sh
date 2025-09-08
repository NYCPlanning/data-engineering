#!/bin/bash

source config.sh
set -e


COMMON_APT_PACKAGES="curl zip unzip git wget ca-certificates lsb-release build-essential sudo postgresql-client libpq-dev jq locales pandoc weasyprint"

apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y -V install $COMMON_APT_PACKAGES \


function install_yq {
    wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq \
        && chmod +x /usr/bin/yq
}

function install_mc {
    curl -O https://dl.min.io/client/mc/release/linux-amd64/mc \
        && chmod +x mc \
        && mv ./mc /usr/bin/ 
}

function install_gdal {
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    sudo apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    sudo apt-get update

    sudo apt-get install -y -V libproj-dev libgeos-dev libarrow-dev libparquet-dev cmake

    echo "installing gdal from source" && (
        cd ~
        wget https://github.com/fvankrieken/gdal/archive/refs/heads/3.8-revert-null-date-behavior.zip
        unzip "3.8-revert-null-date-behavior.zip"
        cd gdal-3.8-revert-null-date-behavior
        mkdir build
        cd build
        sudo cmake -DPROJ_INCLUDE_DIR=/usr/include ..
        sudo cmake --build .
        sudo cmake --build . --target install

        cd ~
        rm -rf gdal-3.8-revert-null-date-behavior
        rm "3.8-revert-null-date-behavior.zip"
    )
    echo "gdal installed successfully"
}

install_yq
install_mc
install_gdal
