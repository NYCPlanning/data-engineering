#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_DIR=$(dirname $FILE_DIR)
ROOT_DIR=$PROJECT_DIR/../..

source $ROOT_DIR/bash/utils.sh
set_error_traps

command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh

# dcpy, the app code, and recipe files are NOT baked into this image — they are mounted from
# the repo and run at runtime (see apps/docker-compose.yml). dcpy-lifecycle's own deps come
# from the build-geosupport base image; this exports dcpy-app-qa's full closure (its own
# deps + dcpy-lifecycle's, already-satisfied duplicates are harmless) from the workspace lock.
cd $ROOT_DIR
uv export --no-dev --no-hashes --no-editable --no-emit-workspace --package dcpy-app-qa \
    -o $PROJECT_DIR/dcpy_requirements.txt
cd - >/dev/null

# Build image - Once we reach some sort of MVP, maybe worth starting versioning. For now, just latest
docker_login
build_and_publish_docker_image $PROJECT_DIR nycplanning/qa-streamlit latest

rm $PROJECT_DIR/dcpy_requirements.txt
