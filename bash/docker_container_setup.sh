#!/bin/bash

mc config host add spaces $AWS_S3_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4

if [[ -z "$CI" ]]; then
    export workspace=/workspace
    python3 -m pip install -e . -c ./python/constraints.txt
    python3 -m pip install -e data-library -c ./python/constraints.txt
else
    export workspace=/__w/data-engineering/data-engineering
    python3 -m pip install . -c ./python/constraints.txt
fi 

git config --global --add safe.directory $workspace
