#!/bin/sh

apk add git-subtree
/bin/setup-ssh

git config --global --add safe.directory "$PWD"
git subtree split -P apps/qaqc -b deploy
git checkout deploy

git push "$GIT_REMOTE_URL" "deploy:refs/heads/$APP_REMOTE_BRANCH" --force 
