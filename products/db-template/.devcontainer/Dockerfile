# See here for base image contents: https://github.com/devcontainers/images/tree/main/src/python
ARG VARIANT="3.11"
FROM mcr.microsoft.com/devcontainers/python:0-${VARIANT}

RUN export DEBIAN_FRONTEND=noninteractive

# Run environment setup script
COPY bash/ /bash/
COPY requirements.txt /.
RUN bash/setup_build_env.sh
