FROM python:3.11-slim as build

RUN export DEBIAN_FRONTEND=noninteractive

# Run environment setup script
COPY test/bash/ /bash/
COPY test/requirements.txt /.
RUN bash/setup_env.sh

# Run dev environment setups
RUN apt-get install -y git
RUN pip install -U black
RUN pip install -U bandit

RUN adduser --disabled-password --gecos '' vscode

USER vscode
