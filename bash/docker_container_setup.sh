#!/bin/bash
set -e

PARENT_DIR=$(dirname "$(readlink -f "$0")")
mkdir -p $HOME

if [[ -z "$CI" ]]; then
    # local dev
    git config --global --add safe.directory /workspace
    editable_install_flag="--editable"
else
    # running in github CI
    git config --global --add safe.directory /__w/data-engineering/data-engineering
    # in case the devcontainer is being used
    git config --global --add safe.directory /home/vscode/workspace
fi 

python3 -m pip install $editable_install_flag . --constraint ./admin/run_environment/constraints.txt --no-deps

# configure minio for use with S3 buckets in Digital Ocean
mc alias set spaces "$AWS_S3_ENDPOINT" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" --api S3v4
