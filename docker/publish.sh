#!/bin/bash

if [ -f "${1}" ]; then
    echo "Publishing '${1}' image"
else
    echo "${1} is not an existing Dockerfile." 
    exit 1
fi

set -e

cp $1 Dockerfile
cp -r ../bash ./bash
cp -r ../python ./python

DOCKER_IMAGE_NAME=nycplanning/$1

echo "$DOCKER_PASSWORD" | docker login -u $DOCKER_USER --password-stdin
# Build image - Once we reach some sort of MVP, maybe worth starting versioning. For now, just latest
docker build \
    --tag $DOCKER_IMAGE_NAME:latest .
docker push $DOCKER_IMAGE_NAME:latest

rm Dockerfile
rm -rf bash
rm -rf python
