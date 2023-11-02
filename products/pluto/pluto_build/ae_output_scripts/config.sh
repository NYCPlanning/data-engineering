#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
ROOT_DIR="${FILE_DIR}/../../../.."

source $ROOT_DIR/bash/utils.sh
