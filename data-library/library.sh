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
    apt-get install -y python3-pip python3-distutils gdal-bin libgdal-dev curl git
    apt-get -y install --reinstall build-essential

    python3 -m pip install poetry
    poetry config virtualenvs.create false --local

    case $1 in 
        dev) ;;
        *) option="--no-dev";;
    esac
    gdal_version=$(gdal-config --version)
    gdal_poetry_version=$(poetry_version gdal)

    if [ $gdal_version != $gdal_poetry_version ]; then
        echo "mismatch between installed gdal binary and gdal version in pyproject.yaml"
        echo "update poetry gdal version to $gdal_version"
        exit 1
    fi
    poetry install $option

    export CPLUS_INCLUDE_PATH=/usr/include/gdal
    export C_INCLUDE_PATH=/usr/include/gdal
}

function poetry_version {
    echo $(poetry show $1) | sed -r 's/^.*version : ([0-9\.]+) .*$/\1/'
}

function poetry_lock {
    poetry add gdal=$(gdal-config --version) --lock
}

case $1 in
    init) init ;;
    setup) 
      shift 1
      set -e
      setup $@;;
    poetry_lock) poetry_lock ;;
    *) library_execute $@;;
esac
