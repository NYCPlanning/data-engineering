#!/bin/bash
# This file must be used with "source" to export to active shell env

function export_ {
    if [[ -z "$CI" ]]; then
        export $1
    else
        echo "$1" >> $GITHUB_ENV
    fi
}

if [[ ! -f "$1" ]] ; then
    echo "File \"$1\" does not exist"
    exit 1
fi

for s in $(cat $1 | yq -o json | jq -r ".vars|to_entries|map(\"\(.key)=\(.value|tostring)\")|.[]" ); do
    export_ $s
done
