#!/bin/bash
source bash/config.sh

DATE=$(date "+%Y-%m-%d")

Upload latest &
Upload $DATE

wait 
echo "Upload Complete"
