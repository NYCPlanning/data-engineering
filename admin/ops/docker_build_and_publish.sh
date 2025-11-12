#!/bin/bash

# Publish a docker image
# 3 arguments
#   1 - image. This must match both folder name in admin/run_environments/docker as well as name of pubished image
#   2 - tag. The tag of the image being published
#   3 - base_tag. Optional. Provided as build-arg to docker build (within which defaults to latest)
#                 This is used when building non-base images off of a base image tag other than latest
#
# This must be run from the root of the project to run correctly.
# Assumes existence of admin/run_environment folder with docker folder and python requirements/constraints inside

image=$1
tag=$2
base_tag=$3

DOCKER_DIR=admin/run_environment/docker
IMAGE_DIR=$DOCKER_DIR/$1

source bash/utils.sh
set_error_traps

# this currently is meant to be only run in CI, not run in one of our containers
# TODO drop pip pin at some point - 25.3 has issue with pip-tools
pip install --upgrade pip==25.2 requests beautifulsoup4 pip-tools

function generate_dcpy_requirements {
    pip-compile ./pyproject.toml -o $IMAGE_DIR/dcpy_requirements.txt -c $IMAGE_DIR/constraints.txt
}

function export_geosupport_versions {
    VERSIONSTRING=$(python3 $DOCKER_DIR/geosupport_versions.py)
    echo $VERSIONSTRING
    export $(echo "$VERSIONSTRING" | sed 's/#.*//g' | xargs)
    export VERSION=$MAJOR.$MINOR.$PATCH

    export BUILD_ARGS="--build-arg RELEASE=$RELEASE --build-arg MAJOR=$MAJOR --build-arg MINOR=$MINOR --build-arg PATCH=$PATCH"
}

function common {
    DOCKER_IMAGE_NAME=nycplanning/$image
    cp $DOCKER_DIR/../constraints.txt $IMAGE_DIR
    cp $DOCKER_DIR/config.sh $IMAGE_DIR

    docker_login

    if [[ -n $base_tag ]]; then
        base_tag_command="--build-arg base_tag=$base_tag"
    fi

    if [[ -z $tag ]]; then
        COMMAND="build_and_publish_docker_image $IMAGE_DIR $DOCKER_IMAGE_NAME latest $base_tag_command"
        GEO_COMMAND="build_and_publish_versioned_docker_image $IMAGE_DIR $DOCKER_IMAGE_NAME $VERSION $BUILD_ARGS $base_tag_command"
    else
        COMMAND="build_and_publish_docker_image $IMAGE_DIR $DOCKER_IMAGE_NAME $tag $base_tag_command"
        echo "$COMMAND"
        GEO_COMMAND="build_and_publish_docker_image $IMAGE_DIR $DOCKER_IMAGE_NAME $tag $BUILD_ARGS $base_tag_command"
    fi
}


case $image in
    base)
        common
        $COMMAND;;
    dev)
        export_geosupport_versions
        common
        cp $DOCKER_DIR/../requirements.txt $IMAGE_DIR
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
