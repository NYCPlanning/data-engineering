#!/bin/bash

COMMON_APT_PACKAGES="curl zip unzip git"
GEOSUPPORT_APT_PACKAGES="${COMMON_APT_PACKAGES} build-essential"
BUILD_APT_PACKAGES="${COMMON_APT_PACKAGES} postgresql-client-15 libpq-dev jq wget gdal-bin jq locales libgdal-dev"
DEV_APT_PACKAGES="${BUILD_APT_PACKAGES} bash-completion xdg-utils libgdal-dev"

function install_yq {
    wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq \
        && chmod +x /usr/bin/yq
}

function install_mc {
    curl -O https://dl.min.io/client/mc/release/linux-amd64/mc \
        && chmod +x mc \
        && mv ./mc /usr/bin/ 
}

function install_gdal_python_reqs {
    apt-get install -y libgdal-dev \
        && apt-get -y install --reinstall build-essential

    export CPLUS_INCLUDE_PATH=/usr/include/gdal
    export C_INCLUDE_PATH=/usr/include/gdal
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
