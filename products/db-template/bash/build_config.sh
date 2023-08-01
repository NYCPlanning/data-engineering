#!/bin/bash
#
# Used in build scripts to import utility functions, import environment variables,
# and configure data connections.

source bash/build_utils.sh

# Set environemnt variables from .env files
set_env .env version.env

# Add an S3-compatible service to the MinIO configuration with alias "spaces"
mc config host add spaces $DO_S3_ENDPOINT $DO_ACCESS_KEY_ID $DO_SECRET_ACCESS_KEY --api S3v4
