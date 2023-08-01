#!/bin/bash

# Set Date
DATE=$(date "+%Y-%m-01")
VERSION=$DATE
VERSION_PREV=$(date --date="$(date "+%Y-%m-01") - 1 month" "+%Y-%m-01")
