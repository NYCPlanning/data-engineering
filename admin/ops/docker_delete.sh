#!/bin/bash

# deletes a specific tag of a docker image in dockerhub
# big thanks to https://devopscell.com/docker/dockerhub/2018/04/09/delete-docker-image-tag-dockerhub.html
# 2 arguments
#   1 - image
#   2 - tag
ORGANIZATION="nycplanning"
IMAGE="$1"
TAG="$2"

login_data() {
cat <<EOF
{
  "username": "$DOCKER_USERNAME",
  "password": "$DOCKER_PASSWORD"
}
EOF
}

TOKEN=`curl -s -H "Content-Type: application/json" -X POST -d "$(login_data)" "https://hub.docker.com/v2/users/login/" | jq -r .token`

curl "https://hub.docker.com/v2/repositories/${ORGANIZATION}/${IMAGE}/tags/${TAG}/" \
-X DELETE \
-H "Authorization: JWT ${TOKEN}"
