set -e


case $1 in
    base) echo "Publishing '${1}' image" ;;
    *) 
        echo "${command} not found" 
        exit 1;;
esac

cp $1 Dockerfile

DOCKER_IMAGE_NAME=nycplanning/$1

echo "$DOCKER_PASSWORD" | docker login -u $DOCKER_USER --password-stdin
# Build image - Once we reach some sort of MVP, maybe worth starting versioning. For now, just latest
docker build \
    --tag $DOCKER_IMAGE_NAME:latest .
docker push $DOCKER_IMAGE_NAME:latest

rm Dockerfile