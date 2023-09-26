FROM osgeo/gdal:ubuntu-small-3.6.1

RUN apt update && apt install -y python3-pip python3-distutils git graphviz xdg-utils

RUN adduser --disabled-password --gecos '' vscode

USER vscode

RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.3.2
