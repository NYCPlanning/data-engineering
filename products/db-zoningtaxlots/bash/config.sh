#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
ROOT_DIR="${FILE_DIR}/../../.."

source $ROOT_DIR/bash/utils.sh

set_env $ROOT_DIR/.env version.env
set_error_traps

# Set Date
DATE=$(date "+%Y-%m-01")
VERSION=$DATE
VERSION_PREV=$(date --date="$(date "+%Y/%m/01") - 1 month" "+%Y/%m/01")
