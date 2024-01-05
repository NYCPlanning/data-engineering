#!/bin/bash

COMMON_APT_PACKAGES="curl zip unzip git wget ca-certificates lsb-release build-essential"
GEOSUPPORT_APT_PACKAGES="zip unzip build-essential"
BUILD_APT_PACKAGES="postgresql-client-15 libpq-dev jq locales"
DEV_APT_PACKAGES="${BUILD_APT_PACKAGES} bash-completion xdg-utils sudo"

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
    apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    apt-get update

    apt install -y -V libarrow-dev libparquet-dev libproj-dev cmake

    echo "installing gdal from source" && (
        cd ~
        wget download.osgeo.org/gdal/3.8.2/gdal382.zip
        unzip gdal382.zip
        cd gdal-3.8.2
        mkdir build
        cd build
        cmake -DPROJ_INCLUDE_DIR=/usr/include ..
        cmake --build .
        cmake --build . --target install
    )
    


    export CPLUS_INCLUDE_PATH=/usr/include/gdal
    export C_INCLUDE_PATH=/usr/include/gdal
}

function install_arrow {
    sudo apt-get install -y -V ca-certificates lsb-release wget
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    sudo apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    sudo apt-get update
    sudo apt-get install -y -V libarrow-dev
    sudo apt-get install -y -V libparquet-dev
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
