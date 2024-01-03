# library

![Docker Image Size (latest by date)](https://img.shields.io/docker/image-size/nycplanning/library)

Archive datasets to S3 via CLI

`library archive --help`

`library archive --name dcp_boroboundaries --version 22c --output-format csv`

`library archive --name dcp_commercialoverlay --s3 --latest`

`library archive --name sca_e_pct --version 20230425 --output-format postgres --postgres-url $RECIPE_ENGINE`

Because gdal dependencies are difficult to install, we recommend using the `library` CLI commands via our docker image `nycplanning/library:ubuntu-latest`
