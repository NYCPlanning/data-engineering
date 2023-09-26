#!/bin/bash

BUILD_APT_PACKAGES="postgresql-client-15 libpq-dev curl jq wget gdal-bin jq zip unzip git locales"
DEV_APT_PACKAGES="postgresql-client-15 libpq-dev curl jq wget gdal-bin jq zip unzip git locales bash-completion graphviz xdg-utils libgdal-dev"

function install_yq {
    wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq \
        && chmod +x /usr/bin/yq
}

function install_mc {
    curl -O https://dl.min.io/client/mc/release/linux-amd64/mc \
        && chmod +x mc \
        && mv ./mc /usr/bin/ 
}

function install_geosupport {
    mkdir -p /geocode && (
        cd /geocode
        FILE_NAME=linux_geo${RELEASE}_${MAJOR}_${MINOR}.zip
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
