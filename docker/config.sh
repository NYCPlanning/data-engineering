#!/bin/bash

# Logic in this file is used within builds of docker containers

COMMON_APT_PACKAGES="curl zip unzip git wget ca-certificates lsb-release build-essential sudo postgresql-client-15 libpq-dev jq locales"
GEOSUPPORT_APT_PACKAGES="curl git unzip zip build-essential"
DEV_APT_PACKAGES="bash-completion xdg-utils"

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
    )
    echo "gdal installed successfully"
}

function install_geosupport {
    mkdir -p /geocode && (
        cd /geocode
        FILE_NAME=linux_geo${RELEASE}_${MAJOR}.${MINOR}.zip
        echo $FILE_NAME
        curl -O https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/$FILE_NAME
        unzip *.zip
        rm *.zip

        #!/bin/bash
        if [[ ${PATCH} = 0 ]]; then
            echo "NO UPAD AVAILABLE YET ..."
        else
            echo "YES UPAD IS AVAILABLE linux_upad_tpad_${RELEASE}${PATCH}"
            mkdir linux_upad_tpad_${RELEASE}${PATCH} &&
                curl -o linux_upad_tpad_${RELEASE}${PATCH}/linux_upad_tpad_${RELEASE}${PATCH}.zip https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/linux_upad_tpad_${RELEASE}${PATCH}.zip &&
                unzip -o linux_upad_tpad_${RELEASE}${PATCH}/*.zip -d version-${RELEASE}_${MAJOR}.${MINOR}/fls/ &&
                rm -r linux_upad_tpad_${RELEASE}${PATCH}
        fi
    )
}
