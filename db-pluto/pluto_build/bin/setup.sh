#!/bin/bash

function setup {
    sudo add-apt-repository ppa:ubuntugis/ppa && sudo apt-get update
    sudo apt-get install -y curl zip
    sudo apt-get install -y --no-install-recommends gdal-bin

    curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
    chmod +x mc
    sudo mv ./mc /usr/bin
    mc config host add spaces $AWS_S3_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4
}
register 'setup' 'init' 'install all dependencies' setup
