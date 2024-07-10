#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
ROOT_DIR="${FILE_DIR}/../../.."

source $ROOT_DIR/bash/utils.sh

set_env $ROOT_DIR/.env version.env
set_error_traps

# Set Date and Versions
VERSION_PREV=$(date --date=""$VERSION" - 1 month" "+%Y-%m-01")
# Set SQL Version Table Names
VERSION_SQL_TABLE=$(date --date="$VERSION" "+%Y%m01")
VERSION_PREV_SQL_TABLE=$(date --date=""$VERSION" - 1 month" "+%Y%m01")
