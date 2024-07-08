#!/bin/bash

command=$1
project=$2
dir="products/${project}"
folder=${3:-models}

if [ -z $project ]
then
    echo "supply both sqlfluff command (lint/fix) and project folder"
    exit 1
fi

(
    cd $dir
    dbt deps
    sqlfluff $command $dir/$folder --templater=dbt
    dbt build --select config.materialized:seed --indirect-selection=cautious --full-refresh
)
