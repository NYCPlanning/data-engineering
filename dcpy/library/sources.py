import os

from osgeo import gdal

from .utils import parse_engine


def get_allowed_drivers(url: str) -> list:
    """
    Returns allowed drivers for OpenEx
    given the file type of [url]
    """
    allowed_drivers = [
        gdal.GetDriver(i).GetDescription() for i in range(gdal.GetDriverCount())
    ]

    _, extension = os.path.splitext(os.path.basename(url))
    if extension == ".csv":
        allowed_drivers = [driver for driver in allowed_drivers if "JSON" not in driver]
    return allowed_drivers


def postgres_source(url: str) -> gdal.Dataset:
    """
    url: postgres connection string
    e.g. postgresql://username:password@host:port/database
    """
    parsed = parse_engine(url)
    return gdal.OpenEx(parsed, gdal.OF_VECTOR)


def generic_source(path: str, options: list | None = None) -> gdal.Dataset:
    """
    path: filepath, http url or s3 file url
    e.g.
        - s3://edm-recipes/2020-01-02/some-file.csv
        - https://some.website.com/file.csv
        - /local/fodler/abc.csv
    """
    options = options or []
    allowed_drivers = get_allowed_drivers(path)
    dataset = gdal.OpenEx(
        path, gdal.OF_VECTOR, open_options=options, allowed_drivers=allowed_drivers
    )
    assert dataset, f"{path} is invalid"
    return dataset
