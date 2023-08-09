#!/bin/bash
#
# Sets up environment for dev container
set -e
SCRIPT_DIRECTORY=$( dirname -- "$0"; )

echo "Running initializeCommand from $SCRIPT_DIRECTORY/ ..."

# create an empty .env if it doesn't exist to allow use of --env-file in runArgs
echo "If it doesn't exist, create an empty $SCRIPT_DIRECTORY/.env ..."
touch $SCRIPT_DIRECTORY/.env

# only do this when running locally (rather than in a github action)
if [[ ${CI} != "true" ]]; then
	# Adding local keys in order to push to remote repo on github from dev containter
	ssh-add
	# Avoiding git issues due to dubious ownership 
	git config --global --add safe.directory $PWD
fi

echo "Done running initializeCommand"
