from datetime import datetime
from functools import cached_property
from jinja2 import Template
import json
import importlib
from pathlib import Path
import yaml

from dcpy.models.library import DatasetDefinition
from dcpy.connectors.esri import arcgis_feature_service
from dcpy.connectors.socrata.configuration import Org as SocrataOrg
from dcpy.connectors.socrata import extract as socrata
from .utils import format_url
from .validator import Validator, Dataset


class Config:
    """
    Config will take a configuration file from the templates directly
    or any given configuration file and compute/output a configuration
    file to pass into the Ingestor
    """

    def __init__(
        self,
        path: str,
        version: str | None = None,
        source_path_override: str | None = None,
    ):
        self.path = path
        self.version = version
        self.source_path_override = source_path_override

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
    def compute(self) -> DatasetDefinition:
        """based on given yml file, compute the configuration"""

        # Validate unparsed, unrendered file
        Validator(yaml.safe_load(self.unparsed_unrendered_template))()

        _config = self.parsed_unrendered_template
        if _config.source.socrata:
            version = socrata.get_version(
                org=SocrataOrg.nyc, uid=_config.source.socrata.uid
            )
        elif _config.source.arcgis_feature_server:
            fs = _config.source.arcgis_feature_server
            feature_server_layer = arcgis_feature_service.resolve_layer(
                feature_server=fs.feature_server,
                layer_name=fs.layer_name,
                layer_id=fs.layer_id,
            )
            _config.source.arcgis_feature_server = (
                DatasetDefinition.SourceSection.FeatureServerLayerDefinition(
                    **feature_server_layer.model_dump()
                )
            )
            version = arcgis_feature_service.get_data_last_updated(
                feature_server_layer
            ).strftime("%Y%m%d")
        else:
            # backwards compatibility before templates were simplified
            if _config.version and _config.version.replace(" ", "") == r"{{version}}":
                _config.version = None
            version = self.version or _config.version or self.version_today
        config = self.parsed_rendered_template(version=version)

        if config.source.script:
            if self.source_path_override:
                if config.source.script.path:
                    config.source.script.path = self.source_path_override
                else:
                    raise ValueError(
                        "Cannot override path of script dataset without path argument"
                    )

            name = config.source.script.name or config.name
            module = importlib.import_module(f"dcpy.library.script.{name}")
            scriptor = module.Scriptor(config=config.model_dump())
            path = scriptor.runner()
            config.source.gdalpath = format_url(path)

        elif config.source.socrata:
            if self.source_path_override:
                raise ValueError("Cannot override path of socrata dataset")
            source = config.source.socrata
            if source.format == "csv":
                path = f"library/tmp/{config.name}.csv"
            elif source.format == "geojson":
                path = f"library/tmp/{config.name}.geojson"
            elif source.format == "shapefile":
                path = f"library/tmp/{config.name}.zip"
            else:
                raise Exception(
                    "Socrata source format must be 'csv', 'geojson', or 'shapefile'."
                )
            socrata.download(
                org=SocrataOrg.nyc,
                uid=source.uid,
                format=source.format,
                path=Path(path),
            )
            config.source.gdalpath = path

        elif config.source.arcgis_feature_server:
            if self.source_path_override:
                raise ValueError(
                    "Cannot override path of arcgis feature server dataset"
                )
            tmp_dir = Path(__file__).parent / "tmp"
            tmp_dir.mkdir(exist_ok=True)
            if not (config.source.geometry and config.source.geometry.SRS):
                raise Exception("Must provide source crs for arcgis feature server")

            geojson = arcgis_feature_service.get_layer(
                feature_server_layer,
                crs=int(config.source.geometry.SRS.strip("EPSG:")),
            )
            file = tmp_dir / f"{config.name}.geojson"
            with open(tmp_dir / f"{config.name}.geojson", "w") as f:
                json.dump(geojson, f)
            config.source.gdalpath = format_url(str(file))

        elif config.source.url:
            if self.source_path_override:
                config.source.url.path = self.source_path_override
            config.source.gdalpath = format_url(
                config.source.url.path, config.source.url.subpath
            )

        else:
            # this error should not be raised - call to Validator takes care of this
            # however, mypy must be appeased
            raise Exception(
                "Either url, script source, or socrata source must be defined"
            )

        return DatasetDefinition(**config.model_dump())
