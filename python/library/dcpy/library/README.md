# library

![Docker Image Size (latest by date)](https://img.shields.io/docker/image-size/nycplanning/library)

Archive datasets to S3 via CLI

`library archive --help`

`library archive --name dcp_boroboundaries --version 22c --output-format csv`

`library archive --name dcp_commercialoverlay --s3 --latest`

`library archive --name sca_bluebook --version 20240110 --output-format postgres --postgres-url $RECIPE_ENGINE --latest`

Because gdal dependencies are difficult to install, we recommend using the `library` CLI commands via our docker image `nycplanning/library:ubuntu-latest`
