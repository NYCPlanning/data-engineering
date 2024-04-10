#!/bin/bash

function export_all {
    DATE=$(date "+%Y-%m-%d")
    SPACES="spaces/edm-publishing/db-eddt/${branchname}"
    echo $SPACES
    mc cp -r .staging/* $SPACES/$DATE/
    mc cp -r .staging/* $SPACES/latest/
}

function export_category {
    DATE=$(date "+%Y-%m-%d")
    SPACES="spaces/edm-publishing/db-eddt/${branchname}"
    mc cp -r .staging/$1/* $SPACES/$DATE/$1/
    mc cp -r .staging/$1/* $SPACES/latest/$1/
}

if [ $CI ]; then
    if [ $GITHUB_EVENT_NAME == 'pull_request' ]; then
        branchname=$GITHUB_HEAD_REF
    else
        branchname=$GITHUB_REF_NAME
    fi
else 
    branchname=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)
fi

if [ $# -eq 0 ] || [ $1 == 'all' ] || [ $1 == '--github_ref' ]; then
    export_all
else
    export_category $1
fi
