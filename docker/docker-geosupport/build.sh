#!/bin/bash

DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
DOCKER_DIR=$(dirname ${DIR})
ROOT_DIR=$(dirname ${DOCKER_DIR})

source ${ROOT_DIR}/bash/utils.sh
cp ${ROOT_DIR}/python/constraints.txt ${DIR}

# Exit when any command fails
set -e
# Keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# Echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

VERSIONSTRING=$(python3 versions.py)
echo "VERSIONSTRING from versions.py is $VERSIONSTRING"

export $(echo "$VERSIONSTRING" | sed 's/#.*//g' | xargs)
VERSION=$MAJOR.$MINOR.$PATCH

docker_login

build_and_publish_docker_image nycplanning/docker-geosupport $VERSION \
    --build-arg RELEASE=$RELEASE \
    --build-arg MAJOR=$MAJOR \
    --build-arg MINOR=$MINOR \
    --build-arg PATCH=$PATCH \
    $DIR
