#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_DIR=$(dirname $FILE_DIR)
ROOT_DIR=$PROJECT_DIR/../..

source $ROOT_DIR/bash/utils.sh
set_error_traps

cp $ROOT_DIR/pyproject.toml $PROJECT_DIR
cp $ROOT_DIR/admin/run_environment/constraints.txt $PROJECT_DIR
cp -r $ROOT_DIR/dcpy $PROJECT_DIR

# copy in recipe files so source datasets can be determined
# ideally these eventually live in common folder (such as somewhere in dcpy)
for product in $(find products -maxdepth 1 -mindepth 1 -type d);
do
    recipe=$ROOT_DIR/$product/recipe.yml
    [ -f "$recipe" ] && mkdir -p $PROJECT_DIR/$product && cp $recipe $PROJECT_DIR/$product/recipe.yml
done

# Build image - Once we reach some sort of MVP, maybe worth starting versioning. For now, just latest
docker_login
build_and_publish_docker_image $PROJECT_DIR nycplanning/qa-streamlit latest

rm $PROJECT_DIR/pyproject.toml
rm $PROJECT_DIR/constraints.txt
rm -rf $PROJECT_DIR/dcpy
