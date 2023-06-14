#!/bin/bash

function setup {
    sudo apt-get update
    sudo apt-get install -y curl zip
    wget http://download.osgeo.org/gdal/3.6.4/gdal-3.6.4.tar.gz
    tar -xvzf gdal-3.6.4.tar.gz && (
        cd gdal-3.6.4

        ./configure --prefix=/usr 
        make
    )
    rm -rf gdal-3.6.4
    rm gdal-3.6.4.tar.gz

    curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
    chmod +x mc
    sudo mv ./mc /usr/bin
    mc config host add spaces $AWS_S3_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4
}
register 'setup' 'init' 'install all dependencies' setup
