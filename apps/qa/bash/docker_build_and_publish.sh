#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_DIR=$(dirname $FILE_DIR)
ROOT_DIR=$PROJECT_DIR/../..

source $ROOT_DIR/bash/utils.sh
set_error_traps

cp -r $ROOT_DIR/dcpy .
cp -r $ROOT_DIR/python/constraints.txt $PROJECT_DIR

# Build image - Once we reach some sort of MVP, maybe worth starting versioning. For now, just latest
docker_login
build_and_publish_docker_image $PROJECT_DIR nycplanning/qa-streamlit latest

rm -rf ./dcpy
