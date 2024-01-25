from datetime import datetime
import os
import pytz
import subprocess
from urllib.parse import urlparse

from . import TEMPLATE_DIR


def parse_engine(url: str) -> str:
    """
    url: postgres connection string
    e.g. postgresql://username:password@host:port/database
    """
    result = urlparse(url)
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    portnum = result.port
    return f"PG:host={hostname} port={portnum} user={username} dbname={database} password={password}"


def format_url(path: str, subpath: str = "") -> str:
    """
    Adds "vsis3" if [url] is from s3
    - s3://edm-recipes/recipes.csv

    Adds "vsizip" to [url] if [url] is zipped.
    - abcd.zip/abcd.shp
    - abcd.zip/abcd.csv
    - etc

    Adds "vsicurl" if [url] contains http
    - https://rawgithubcontent.come/somerepo/somefile.csv
    """
    subpath = subpath if subpath is not None else ""
    if len(subpath) > 0:
        subpath = subpath[1:] if subpath[0] == "/" else subpath
    path = path[:-1] if path[-1] == "/" else path
    url = path if len(subpath) == 0 else path + "/" + subpath

    if "s3://" in url:
        url = url.replace("s3://", "/vsis3/")

    if ".zip" in url:
        if "http" in url:
            url = "/vsicurl/" + url
        url = "/vsizip/" + url

    return url


def get_execution_details() -> dict[str, str]:
    def try_func(func):
        try:
            return func()
        except:
            return "could not parse"

    timestamp = datetime.now(pytz.timezone("America/New_York")).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    if os.environ.get("CI"):
        return {
            "type": "ci",
            "dispatch_event": os.environ.get("GITHUB_EVENT_NAME", "could not parse"),
            "url": try_func(
                lambda: f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
            ),
            "job": os.environ.get("GITHUB_JOB", "could not parse"),
            "timestamp": timestamp,
        }
    else:
        git_user = try_func(
            lambda: subprocess.run(
                ["git", "config", "user.name"], stdout=subprocess.PIPE
            )
            .stdout.strip()
            .decode()
        )
        return {
            "type": "manual",
            "user": git_user,
            "timestamp": timestamp,
        }


def get_all_templates():
    """Get names of all dataset templates included in dcpy"""
    return [file.stem for file in TEMPLATE_DIR.glob("*")]
