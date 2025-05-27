#!/bin/bash
set -e
mc alias set spaces "$AWS_S3_ENDPOINT" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" --api S3v4

PARENT_DIR=$(dirname "$(readlink -f "$0")")

if [[ -z "$CI" ]]; then
    export workspace=/workspace
    option="-e"
else
    export workspace=/__w/data-engineering/data-engineering
fi 

python3 -m pip install $option . -c ./admin/run_environment/constraints.txt --no-deps

git config --global --add safe.directory $workspace
