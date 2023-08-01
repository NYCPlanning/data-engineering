#!/bin/bash

function setup {
    apt-get update
    apt-get install -y curl zip jq unzip postgresql-client

    curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
    chmod +x mc
    mv ./mc /usr/bin
    mc config host add spaces $AWS_S3_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4
}
register 'setup' 'init' 'install all dependencies' setup
