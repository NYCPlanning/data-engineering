import importlib
import json
from datetime import datetime
import requests
import yaml
from jinja2 import Template
import os
from typing import Optional, Tuple
from functools import cached_property

from .utils import format_url, get_execution_details
from .validator import Validator


# Custom dumper created for list indentation
class Dumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(Dumper, self).increase_indent(flow, False)


class Config:
    """
    Config will take a configuration file from the templates directly
    or any given configuration file and compute/output a configuration
    file to pass into the Ingestor
    """

    def __init__(self, path: str, version: Optional[str] = None):
        self.path = path
        self.version = version

    @property
    def unparsed_unrendered_template(self) -> str:
        """importing the yml file into a string"""
        with open(self.path, "r") as f:
            return f.read()

    @property
    def parsed_unrendered_template(self) -> dict:
        """parsing unrendered template into a dictionary"""
        return yaml.safe_load(self.unparsed_unrendered_template)

    def parsed_rendered_template(self, **kwargs) -> dict:
        """render template, then parse into a dictionary"""
        template = Template(self.unparsed_unrendered_template)
        return yaml.safe_load(template.render(**kwargs))

    @property
    def source_type(self) -> str:
        """determine the type of the source, either url, socrata or script"""
        template = self.parsed_unrendered_template
        source = template["dataset"]["source"]
        return list(source.keys())[0]

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

    def valid_version(self, version: str) -> bool:
        """check that a version name is valid"""
        return "{" not in version and "}" not in version

    @property
    def version_today(self) -> str:
        """
        set today as the version name - for use with unspecified
        or invalid versions
        """
        return datetime.today().strftime("%Y%m%d")

    @cached_property
    def compute(self) -> dict:
        """based on given yml file, compute the configuration"""

        # Validate unparsed, unrendered file
        Validator(self.parsed_unrendered_template)()

        if self.source_type == "script":
            if self.version:
                version = self.version
            else:
                version = self.version_today
            config = self.parsed_rendered_template(version=version)
            _config = self.parsed_unrendered_template

            script_name = _config["dataset"]["source"]["script"]
            module = importlib.import_module(f"dcpy.library.script.{script_name}")
            scriptor = module.Scriptor(config=config)
            url = scriptor.runner()

            options = config["dataset"]["source"]["options"]
            geometry = config["dataset"]["source"]["geometry"]
            config["dataset"]["source"] = {
                "url": {"path": url, "subpath": ""},
                "options": options,
                "geometry": geometry,
            }

        if self.source_type == "url":
            # Load unrendered template to check for yml-specified
            # version (_version)
            _config = self.parsed_unrendered_template
            _version = _config["dataset"]["version"]

            # If a custom version specified from CLI, take custom version
            if self.version:
                version = self.version

            # If no custom version specified and version in config
            # is valid, take config version (_version)
            if not self.version and self.valid_version(_version):
                version = _version

            # If no custom version and no config version,
            # assign today as version
            if not self.version and not self.valid_version(_version):
                version = self.version_today

            # Render template
            config = self.parsed_rendered_template(version=version)

            # Force overwrite of yml version with appropriate version
            config["dataset"]["version"] = version

        if self.source_type == "socrata":
            # For socrata we are computing the url and add the url object to the config file
            _uid = self.parsed_unrendered_template["dataset"]["source"]["socrata"][
                "uid"
            ]
            _format = self.parsed_unrendered_template["dataset"]["source"]["socrata"][
                "format"
            ]
            config = self.parsed_rendered_template(version=self.version_socrata(_uid))

            if _format == "csv":
                url = f"https://data.cityofnewyork.us/api/views/{_uid}/rows.csv"
            if _format == "geojson":
                url = f"https://nycopendata.socrata.com/api/geospatial/{_uid}?method=export&format=GeoJSON"
            if _format == "shapefile":
                local_path = f"library/tmp/{config['dataset']['name']}.zip"
                url = f"https://data.cityofnewyork.us/api/geospatial/{_uid}?method=export&format=Shapefile"
                os.system("mkdir -p library/tmp")
                os.system(f'curl -o {local_path} "{url}"')
                url = local_path

            options = config["dataset"]["source"]["options"]
            geometry = config["dataset"]["source"]["geometry"]
            config["dataset"]["source"] = {
                "url": {"path": url, "subpath": ""},
                "options": options,
                "geometry": geometry,
            }

        path = config["dataset"]["source"]["url"]["path"]
        subpath = config["dataset"]["source"]["url"]["subpath"]
        config["dataset"]["source"]["url"]["gdalpath"] = format_url(path, subpath)
        config["execution_details"] = get_execution_details()
        return config

    @property
    def compute_json(self) -> str:
        return json.dumps(self.compute, indent=4)

    @property
    def compute_yml(self) -> str:
        return yaml.dump(
            self.compute,
            Dumper=Dumper,
            default_flow_style=False,
            sort_keys=False,
            indent=2,
        )

    @property
    def compute_parsed(self) -> Tuple[dict, dict, dict, dict]:
        config = self.compute
        dataset = config["dataset"]
        source = dataset["source"]
        destination = dataset["destination"]
        info = dataset["info"]
        return dataset, source, destination, info
