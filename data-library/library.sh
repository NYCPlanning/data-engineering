#!/bin/bash
function init {
    UUID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 10 | head -n 1)
    docker build . -t library:$UUID
    echo "$UUID" > .config
}

function library_execute {
    if [ ! -f .config ]; then
        echo "run ./library.sh init first to initialize"
    else
        UUID=$(cat .config)
        docker run --rm --tty --env-file .env\
            -u $(id -u ${USER}):$(id -g ${USER})\
            -v $(pwd):/library library:$UUID library $@
    fi
}

case $1 in
    init) init ;;
    *) library_execute $@;;
esac