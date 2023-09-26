# db-data-library
![Docker Image Size (latest by date)](https://img.shields.io/docker/image-size/nycplanning/library)

Archive datasets to S3 via CLI

`library archive --help`

`library archive --name dcp_boroboundaries --version 22c --output-format csv`

`library archive --name dcp_commercialoverlay --s3 --latest`

`library archive --name sca_e_pct --version 20230425 --output-format postgres --postgres-url $RECIPE_ENGINE`

## Usage

Because gdal dependencies are difficult to install, we recommend using the `library` CLI commands via our docker image `nycplanning/library:ubuntu-latest`

### Method A: Run a single command

If you have environmental variables stored in a `.env` file:
```bash
docker run --rm --env-file .env \
    nycplanning/library:ubuntu-latest < library ... >
```

Otherwise, use `docker run` with explicit environmental variables:
```bash
docker run --rm\
    -e AWS_S3_ENDPOINT=< endpoint >
    -e AWS_SECRET_ACCESS_KEY=< access secret ket >
    -e AWS_ACCESS_KEY_ID=< access key id >
    -e AWS_S3_BUCKET=< bucket name >
    nycplanning/library:ubuntu-latest < library ... >
```

> Where the `library ...` command can be any of the `library` commands (e.g.
`library archive --name dcp_commercialoverlay -s -c`)

### Method B: Use a dev container in VS Code

1. Open the repo in a `Remote Window` in VS Code (either when prompted or via the green icon at the bottom left)

2. Start a poetry shell via `poetry shell`

3. Install python packages via `poetry install`

4. Run `library` commands

### Method C: Use github actions
>💡 Note: This method will always push to S3

1. Navigate to the `Actions` section of the repo

2. Select `Update a Single Dataset`

3. Within `Run workflow`, enter the relevant inputs and click `Run workflow`


## Dev Instructions

1. Make sure you have GDAL installed (we are using version `3.2.1+dfsg-1+b1`)
```bash
sudo apt install -y gdal-bin libgdal-dev python3-gdal
```
2. then install poetry
```bash
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 -
```
3. Use poetry to install dependencies `poetry install`
4. Install pre-commit `poetry run pre-commit install`
5. Check out what's available via the cli `poetry run library --help`
6. To add/update documentation, run `poetry run pdoc -o docs --html library`

## Testing

To test all functions within a script:
`poetry run pytest tests/{test script}.py -s`

To test a specific function:
`poetry run pytest tests/{test script}.py::{test name} -s`
> note the `-s` flag is optional, it allows print output (via stdout) to be included in the test output, otherwise it is ignored
