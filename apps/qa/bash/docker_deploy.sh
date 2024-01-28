#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_DIR=$(dirname $FILE_DIR)
ROOT_DIR=$PROJECT_DIR/../..

source $ROOT_DIR/bash/utils.sh
set_error_traps

mkdir -p ~/.ssh
echo "$QA_DROPLET_SSH_PRIVATE_KEY" > ~/.ssh/qa.key
chmod 400 ~/.ssh/qa.key
cat >> ~/.ssh/config <<EOF
    Host qa
    HostName $QA_DROPLET_SSH_HOST
    User root
    IdentityFile ~/.ssh/qa.key
    StrictHostKeyChecking no
EOF

ssh qa "\
    set -e
    docker info
    docker images
    docker pull $DOCKER_IMAGE_NAME
    pid=\"\$(docker ps -a -q)\"
    if [[ \$pid ]]; then
        docker stop \"\$pid\"
        docker rm \"\$pid\"
    fi
    docker run -p 8501:8501 -d --restart always\
        -e AWS_S3_ENDPOINT=$AWS_S3_ENDPOINT\
        -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY\
        -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID\
        -e BUILD_ENGINE_SERVER=$BUILD_ENGINE_SERVER\
        -e GHP_TOKEN=$GHP_TOKEN\
        $DOCKER_IMAGE_NAME"
