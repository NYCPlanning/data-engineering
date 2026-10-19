#!/bin/bash

# replace dashes with underscores to create a valid postgres schema name
export BUILD_ENGINE_SCHEMA=$(echo ${BUILD_NAME} | tr - _ | tr '[:upper:]' '[:lower:]')
# align with tests schema name created by dbt
export BUILD_ENGINE_SCHEMA_TESTS=$(echo ${BUILD_ENGINE_SCHEMA}__tests)
echo "BUILD_ENGINE_SCHEMA=$BUILD_ENGINE_SCHEMA" >> "$GITHUB_ENV"
echo "BUILD_ENGINE_SCHEMA_TESTS=$BUILD_ENGINE_SCHEMA_TESTS" >> "$GITHUB_ENV"

# set postgres schema search path to prioritize BUILD_ENGINE_SCHEMA
export BUILD_ENGINE=$(./bash/bin/echo_build_engine)
echo "BUILD_ENGINE=$BUILD_ENGINE" >> "$GITHUB_ENV"
