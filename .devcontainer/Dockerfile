FROM ghcr.io/osgeo/gdal:ubuntu-small-3.6.4

ENV PYTHONUNBUFFERED 1

RUN adduser --disabled-password --gecos '' vscode

# Update args in docker-compose.yaml to set the UID/GID of the "vscode" user.
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN if [ "$USER_GID" != "1000" ] || [ "$USER_UID" != "1000" ]; then groupmod --gid $USER_GID vscode && usermod --uid $USER_UID --gid $USER_GID vscode; fi

RUN apt update && export DEBIAN_FRONTEND=noninteractive \
    && apt -y install postgresql-client libpq-dev python3-pip python3-distutils jq zip unzip git bash-completion locales

ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en 

RUN curl -O https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x mc \
    && mv ./mc /usr/bin/ 

# need to load env if this happens here
# RUN mc config host add spaces $AWS_S3_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4

USER vscode