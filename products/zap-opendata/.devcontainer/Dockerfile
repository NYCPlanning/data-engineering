# See here for base image contents: https://github.com/devcontainers/images/tree/main/src/python
ARG VARIANT="3.9"

FROM mcr.microsoft.com/devcontainers/python:0-${VARIANT}

FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:432.0.0-slim

RUN export DEBIAN_FRONTEND=noninteractive

# Run build environment setup script
COPY bash/ /bash/
COPY requirements.txt /.
RUN bash/setup_build_env.sh
