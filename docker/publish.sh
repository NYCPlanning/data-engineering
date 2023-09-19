#!/bin/bash

PYTHON_VERSION="3.11"

DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
ROOT_DIR=$(dirname ${DIR})

source $ROOT_DIR/bash/utils.sh
set_error_traps

pip3 install requests beautifulsoup4

function export_geosupport_versions {
    VERSIONSTRING=$(python3 geosupport_versions.py)

    export $(echo "$VERSIONSTRING" | sed 's/#.*//g' | xargs)
    export VERSION=$MAJOR.$MINOR.$PATCH

    export BUILD_ARGS="--build-arg RELEASE=$RELEASE --build-arg MAJOR=$MAJOR --build-arg MINOR=$MINOR --build-arg PATCH=$PATCH"
}

function common {
    DOCKER_IMAGE_NAME=nycplanning/$1
    cp $ROOT_DIR/python/constraints.txt $1
    cp -r $ROOT_DIR/dcpy $1
    cp $DIR/config.sh $1

    docker_login
}

case $1 in
    dev)
        common $1
        cp $ROOT_DIR/python/requirements.txt $1
        export_geosupport_versions
        build_and_publish_versioned_docker_image $1 $DOCKER_IMAGE_NAME $VERSION $BUILD_ARGS --build-arg PYTHON_VERSION=$PYTHON_VERSION;;
    build-base) 
        common $1
        build_and_publish_docker_image $1 $DOCKER_IMAGE_NAME latest --build-arg PYTHON_VERSION=$PYTHON_VERSION;;
    build-geosupport) 
        common $1
        export_geosupport_versions
        build_and_publish_versioned_docker_image $1 $DOCKER_IMAGE_NAME $VERSION $BUILD_ARGS --build-arg PYTHON_VERSION=$PYTHON_VERSION;;
    docker-geosupport)
        common $1
        export_geosupport_versions
        build_and_publish_versioned_docker_image $1 $DOCKER_IMAGE_NAME $VERSION $BUILD_ARGS --build-arg PYTHON_VERSION=$PYTHON_VERSION;;
    *)
        echo "${1} is not an valid Dockerfile." 
        exit 1;;
esac
