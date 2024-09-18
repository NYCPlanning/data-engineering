#!/bin/bash

image=$1
tag=$2
base_tag=$3

DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
DOCKER_DIR="${DIR}/$1"

source bash/utils.sh
set_error_traps

pip3 install requests beautifulsoup4 pip-tools

function generate_dcpy_requirements {
    pip-compile ../pyproject.toml -o $DOCKER_DIR/dcpy_requirements.txt -c $DOCKER_DIR/constraints.txt
}

function export_geosupport_versions {
    VERSIONSTRING=$(python3 geosupport_versions.py)
    echo $VERSIONSTRING
    export $(echo "$VERSIONSTRING" | sed 's/#.*//g' | xargs)
    export VERSION=$MAJOR.$MINOR.$PATCH

    export BUILD_ARGS="--build-arg RELEASE=$RELEASE --build-arg MAJOR=$MAJOR --build-arg MINOR=$MINOR --build-arg PATCH=$PATCH"
}

function common {
    DOCKER_IMAGE_NAME=nycplanning/$image
    cp $DIR/../python/constraints.txt $DOCKER_DIR
    cp $DIR/config.sh $DOCKER_DIR

    docker_login

    if [[ -n $base_tag ]]; then
        base_tag_command="--build-arg base_tag=$base_tag"
    fi

    if [[ -z $tag ]]; then
        COMMAND="build_and_publish_docker_image $DOCKER_DIR $DOCKER_IMAGE_NAME latest $base_tag_command"
        GEO_COMMAND="build_and_publish_versioned_docker_image $DOCKER_DIR $DOCKER_IMAGE_NAME $VERSION $BUILD_ARGS $base_tag_command"
    else
        COMMAND="build_and_publish_docker_image $DOCKER_DIR $DOCKER_IMAGE_NAME $tag $base_tag_command"
        echo "$COMMAND"
        GEO_COMMAND="build_and_publish_docker_image $DOCKER_DIR $DOCKER_IMAGE_NAME $tag $BUILD_ARGS $base_tag_command"
    fi
}


case $image in
    base)
        common
        $COMMAND;;
    dev)
        export_geosupport_versions
        common
        cp $DIR/../python/requirements.txt $1
        $GEO_COMMAND;;
    build-base) 
        common
        generate_dcpy_requirements
        $COMMAND;;
    build-geosupport) 
        export_geosupport_versions
        common
        generate_dcpy_requirements
        $GEO_COMMAND;;
    docker-geosupport)
        export_geosupport_versions
        common
        $GEO_COMMAND;;
    *)
        echo "$image is not an valid Dockerfile." 
        exit 1;;
esac
