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

if [ -eq "$project" "zoningtaxlots" ]
then
    export BUILD_ENGINE_DB="db-ztl"
else
    export BUILD_ENGINE_DB="db-$(echo "$project" | tr '_' '-')"
fi

echo "
[tool.sqlfluff.templater.dbt]
dialect = \"postgres\"
project_dir = \"${dir}\"
profiles_dir = \"${dir}\"" >> pyproject.toml

dbt deps --profiles-dir $dir --project-dir $dir
dbt build --select config.materialized:seed --indirect-selection=cautious --full-refresh --profiles-dir $dir --project-dir $dir
sqlfluff $command $dir/$folder --templater=dbt

echo "$(head -n -5 pyproject.toml)" > pyproject.toml
