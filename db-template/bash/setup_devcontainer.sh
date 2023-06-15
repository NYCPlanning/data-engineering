#!/bin/bash
#
# Sets up environment for dev in local devcontainer.
set -e

# Skip when called in a github action workflow
if [[ ${CI} != "true" ]]; then
	# Add local SSH private keys in order to push to github from the dev container
	ssh-add
fi
