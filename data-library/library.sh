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

function setup {
    apt update
    apt-get install -y python3-pip python3-distutils gdal-bin libgdal-dev curl git unzip
    apt-get -y install --reinstall build-essential
    
    pip install -e . -c ${1:-constraints.txt}

    export CPLUS_INCLUDE_PATH=/usr/include/gdal
    export C_INCLUDE_PATH=/usr/include/gdal
}


case $1 in
    init) init ;;
    setup) 
      shift 1
      set -e
      setup $@;;
    *) library_execute $@;;
esac
