#!/bin/bash
# Exit when any command fails
set -e
# Keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# Echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

function docker_tag_exists() {
    curl --silent -f -lSL https://index.docker.io/v1/repositories/$1/tags/$2 >/dev/null
}

VERSIONSTRING=$(python3 versions.py)
echo "VERSIONSTRING from versions.py is $VERSIONSTRING"

export $(echo "$VERSIONSTRING" | sed 's/#.*//g' | xargs)
export VERSION=$MAJOR.$MINOR.$PATCH

if docker_tag_exists nycplanning/docker-geosupport $VERSION; then
    echo "nycplanning/docker-geosupport:$VERSION already exist"
else
    # State version name
    echo "$VERSIONSTRING"
    echo "$VERSION"

    # Log into Github registry
    echo "$GITHUB_TOKEN" | docker login ghcr.io -u $GITHUB_ACTOR --password-stdin

    GITHUB_IMAGE_NAME=ghcr.io/nycplanning/docker-geosupport/geosupport
    DOCKER_IMAGE_NAME=nycplanning/docker-geosupport

    # Build image
    docker build \
        --build-arg RELEASE=$RELEASE \
        --build-arg MAJOR=$MAJOR \
        --build-arg MINOR=$MINOR \
        --build-arg PATCH=$PATCH \
        --tag $GITHUB_IMAGE_NAME:${VERSION} .
    docker push $GITHUB_IMAGE_NAME:${VERSION}

    # Push image
    docker tag $GITHUB_IMAGE_NAME:${VERSION} \
        $GITHUB_IMAGE_NAME:latest
    docker push $GITHUB_IMAGE_NAME:latest

    # Log into Docker registry
    echo "$DOCKER_PASSWORD" | docker login -u $DOCKER_USER --password-stdin
    # Update Dockerhub
    docker tag $GITHUB_IMAGE_NAME:${VERSION} \
        $DOCKER_IMAGE_NAME:${VERSION}
    docker push $DOCKER_IMAGE_NAME:${VERSION}
    docker tag $DOCKER_IMAGE_NAME:${VERSION} $DOCKER_IMAGE_NAME:latest
    docker push $DOCKER_IMAGE_NAME:latest
fi
