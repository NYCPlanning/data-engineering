#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")

source $FILE_DIR/../../bash/utils.sh
set_error_traps

# Setting Environmental Variables
set_env .env version.env
DATE=$(date "+%Y-%m-%d")
