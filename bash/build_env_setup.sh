#!/bin/bash

# replace dashes with underscores to create a valid postgres schema name
export BUILD_ENGINE_SCHEMA=$(echo ${BUILD_NAME} | tr - _)
echo "BUILD_ENGINE_SCHEMA=$BUILD_ENGINE_SCHEMA" >> "$GITHUB_ENV"

# set postgres schema search path to prioritize BUILD_ENGINE_SCHEMA
export BUILD_ENGINE=${BUILD_ENGINE_SERVER}/${BUILD_ENGINE_DB}?options=--search_path%3D${BUILD_ENGINE_SCHEMA},public
echo "BUILD_ENGINE=$BUILD_ENGINE" >> "$GITHUB_ENV"
