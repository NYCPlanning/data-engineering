#!/bin/bash

set -e
FILE_DIR=$(dirname "$(readlink -f "$0")")

DOCKER_IMAGE_NAME=nycplanning/qaqc

cp -r $FILE_DIR/../../../dcpy $FILE_DIR/dcpy

echo "$DOCKER_PASSWORD" | docker login -u $DOCKER_USER --password-stdin
# Build image - Once we reach some sort of MVP, maybe worth starting versioning. For now, just latest
docker build \
    --tag $DOCKER_IMAGE_NAME:latest $FILE_DIR/..
docker push $DOCKER_IMAGE_NAME:latest

rm -rf ./dcpy
