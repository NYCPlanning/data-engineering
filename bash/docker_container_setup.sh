#!/bin/bash

mc config host add spaces $AWS_S3_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4

if [[ -z "$CI" ]]; then
    workspace=/workspace
else
    workspace=/__w/data-engineering/data-engineering
    option="-e"
    python3 -m pip install -e data-library -c ./python/constraints.txt
fi 

git config --global --add safe.directory $workspace

python3 -m pip install $option . -c ./python/constraints.txt
