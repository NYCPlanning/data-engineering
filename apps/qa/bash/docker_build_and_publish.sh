#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_DIR=$(dirname $FILE_DIR)
ROOT_DIR=$PROJECT_DIR/../..

source $ROOT_DIR/bash/utils.sh
set_error_traps

# dcpy, the app code, and recipe files are NOT baked into this image — they are mounted from
# the repo and run at runtime (see apps/docker-compose.yml). dcpy's deps come from the
# build-base base image; the build only needs constraints to pin the QA app's own deps.
cp $ROOT_DIR/admin/run_environment/constraints.txt $PROJECT_DIR

# Build image - Once we reach some sort of MVP, maybe worth starting versioning. For now, just latest
docker_login
build_and_publish_docker_image $PROJECT_DIR nycplanning/qa-streamlit latest

rm $PROJECT_DIR/constraints.txt
