#!/bin/bash
#
# Sets up enviroment for running build scripts in either local devcontainer or github action.
set -e

apt-get update

# Install R
apt-get -y install r-base

# Install python packages
python3 -m pip install --upgrade pip
python3 -m pip install --requirement requirements.txt

# Install required R package
Rscript -e "install.packages('survey')"

export $(cat .env | sed 's/#.*//g' | xargs)

curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
mv ./mc /usr/bin
mc config host add spaces $AWS_S3_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4