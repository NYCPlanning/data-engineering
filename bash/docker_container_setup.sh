#!/bin/bash
set -e

PARENT_DIR=$(dirname "$(readlink -f "$0")")
mkdir -p $HOME

if [[ -z "$CI" ]]; then
    # local dev
    git config --global --add safe.directory /workspace
    editable_install_flag="--editable"
else
    # running in github CI
    git config --global --add safe.directory /__w/data-engineering/data-engineering
    # in case the devcontainer is being used
    git config --global --add safe.directory /home/vscode/workspace
fi 

python3 -m pip install $editable_install_flag . --constraint ./admin/run_environment/constraints.txt --no-deps
