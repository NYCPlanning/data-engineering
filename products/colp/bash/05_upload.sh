#!/bin/bash

DATE=$(date "+%Y-%m-%d")

upload db-colp latest &
upload db-colp ${DATE}

wait 
echo "Upload Complete"
