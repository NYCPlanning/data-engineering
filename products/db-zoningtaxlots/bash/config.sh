#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
ROOT_DIR="${FILE_DIR}/../../.."

source $ROOT_DIR/bash/utils.sh

set_env $ROOT_DIR/.env version.env
set_error_traps

# Set Date and Versions
DATE=$(date "+%Y-%m-01")
VERSION=$DATE
VERSION_PREV=$(date --date="$(date "+%Y-%m-01") - 1 month" "+%Y-%m-01")
# Set SQL Version Table Names
VERSION_SQL_TABLE=$(date "+%Y_%m_01")
VERSION_PREV_SQL_TABLE=$(date --date="$(date "+%Y_%m_01") - 1 month" "+%Y_%m_01")
