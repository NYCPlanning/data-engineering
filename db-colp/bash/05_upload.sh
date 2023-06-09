#!/bin/bash
source ../bash_utils/config.sh
set_env ../.env

DATE=$(date "+%Y-%m-%d")

upload db-colp latest &
upload db-colp ${DATE}

wait 
echo "Upload Complete"
