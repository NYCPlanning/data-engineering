#!/bin/bash

# Logic in this file is used within builds of docker containers

GEOSUPPORT_APT_PACKAGES="curl git unzip zip build-essential"
DEV_APT_PACKAGES="bash-completion xdg-utils"

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
                curl -o linux_upad_tpad_${RELEASE}${PATCH}/linux_upad_tpad_${RELEASE}${PATCH}.zip https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/geosupport/linux_upad_tpad_${RELEASE}${PATCH}.zip &&
                unzip -o linux_upad_tpad_${RELEASE}${PATCH}/*.zip -d version-${RELEASE}_${MAJOR}.${MINOR}/fls/ &&
                rm -r linux_upad_tpad_${RELEASE}${PATCH}
        fi
    )
}
