#!/bin/bash
#
# Used in build scripts to import utility functions, import environment variables,
# and configure data connections.

FILE_DIR=$(dirname "$(readlink -f "$0")")
ROOT_DIR="${FILE_DIR}/../../.."

source $ROOT_DIR/bash/utils.sh

# Set environemnt variables from .env files
set_env $ROOT_DIR/.env version.env
set_error_traps
