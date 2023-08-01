#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
ROOT_DIR=$FILE_DIR/../../../

source $ROOT_DIR/bash/utils.sh
set_error_traps

# Setting Environmental Variables
set_env $ROOT_DIR/.env version.env
DATE=$(date "+%Y-%m-%d")
