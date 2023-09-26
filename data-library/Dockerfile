FROM osgeo/gdal:ubuntu-small-3.6.1

COPY . /library/

WORKDIR /library/

RUN apt update && apt install -y python3-pip python3-distutils

RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.3.2

RUN export PATH="/root/.local/bin:$PATH" &&\
    poetry config virtualenvs.create false --local &&\
    poetry install --no-dev
