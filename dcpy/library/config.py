from datetime import datetime
from functools import cached_property
from jinja2 import Template
import importlib
import os
import pytz
import requests
import subprocess
import yaml

from .utils import format_url
from .validator import Validator, Dataset


class Config:
    """
    Config will take a configuration file from the templates directly
    or any given configuration file and compute/output a configuration
    file to pass into the Ingestor
    """

    def __init__(self, path: str, version: str | None = None):
        self.path = path
        self.version = version

    @property
    def unparsed_unrendered_template(self) -> str:
        """importing the yml file into a string"""
        with open(self.path, "r") as f:
            return f.read()

    @property
    def parsed_unrendered_template(self) -> Dataset:
        """parsing unrendered template into a dictionary"""
        return Dataset(**yaml.safe_load(self.unparsed_unrendered_template)["dataset"])

    def parsed_rendered_template(self, version: str) -> Dataset:
        """render template, then parse into a dictionary"""
        template = Template(self.unparsed_unrendered_template)
        loaded = yaml.safe_load(template.render(version=version))["dataset"]
        loaded["version"] = version
        return Dataset(**loaded)

    def version_socrata(self, uid: str) -> str:
        """using the socrata API, collect the 'data last update' date"""
        metadata = requests.get(
            f"https://data.cityofnewyork.us/api/views/{uid}.json"
        ).json()
        version = datetime.fromtimestamp(metadata["rowsUpdatedAt"]).strftime("%Y%m%d")
        return version

    # @property
    # def version_bytes(self) -> str:
    #     """parsing bytes of the big apple to get the latest bytes version"""
    #     # scrape from bytes to get a version
    #     return None

    @property
    def version_today(self) -> str:
        """
        set today as the version name - for use with unspecified
        or invalid versions
        """
        return datetime.today().strftime("%Y%m%d")

    @cached_property
    def compute(self) -> Dataset:
        """based on given yml file, compute the configuration"""

        # Validate unparsed, unrendered file
        Validator(yaml.safe_load(self.unparsed_unrendered_template))()

        _config = self.parsed_unrendered_template
        if _config.source.socrata:
            version = self.version_socrata(_config.source.socrata.uid)
        else:
            # backwards compatibility before templates were simplified
            if _config.version and _config.version.replace(" ", "") == r"{{version}}":
                _config.version = None
            version = self.version or _config.version or self.version_today
        config = self.parsed_rendered_template(version=version)

        # need to be a bit explicit because of truthiness of empty dicts
        if _config.source.script is not None:
            module = importlib.import_module(f"dcpy.library.script.{_config.name}")
            scriptor = module.Scriptor(config=config.model_dump())
            path = scriptor.runner()
            config.source.gdalpath = format_url(path)

        elif _config.source.socrata:
            socrata = _config.source.socrata
            if socrata.format == "csv":
                path = f"https://data.cityofnewyork.us/api/views/{socrata.uid}/rows.csv"
            elif socrata.format == "geojson":
                path = f"https://nycopendata.socrata.com/api/geospatial/{socrata.uid}?method=export&format=GeoJSON"
            elif socrata.format == "shapefile":
                path = f"library/tmp/{config.name}.zip"
                url = f"https://data.cityofnewyork.us/api/geospatial/{socrata.uid}?method=export&format=Shapefile"
                os.system("mkdir -p library/tmp")
                os.system(f'curl -o {path} "{url}"')
            else:
                raise Exception(
                    "Socrata source format must be 'csv', 'geojson', or 'shapefile'."
                )
            config.source.gdalpath = format_url(path)

        elif _config.source.url:
            config.source.gdalpath = format_url(
                _config.source.url.path, _config.source.url.subpath
            )

        else:
            # this error should not be raised - call to Validator takes care of this
            # however, mypy must be appeased
            raise Exception(
                "Either url, script source, or socrata source must be defined"
            )

        config.execution_details = get_execution_details()
        return config


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
