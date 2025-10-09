import pytest
from unittest.mock import patch
import yaml

from dcpy.models.library import DatasetDefinition
from dcpy.library.config import Config
from dcpy.library import TEMPLATE_DIR

from dcpy.test.conftest import mock_request_get
from . import template_path, get_config_file

FAKE_PATH = "./fake_file.csv"


def test_model_dump():
    """Ensure that custom serializers don't mutate form of data"""
    for file in TEMPLATE_DIR.glob("*"):
        with open(file, "r") as f:
            config_dict = yaml.safe_load(f)["dataset"]
        # version is typically set in code after reading in raw template
        if "version" not in config_dict:
            config_dict["version"] = "dummy"
        config = DatasetDefinition(**config_dict)
        yml_str = yaml.dump(config.model_dump(mode="json"))
        _config2 = DatasetDefinition(**yaml.safe_load(yml_str))


def test_config_parsed_rendered_template():
    c = Config(get_config_file("url"))
    rendered = c.parsed_rendered_template(version="20v7")
    assert rendered.version == "20v7"


@patch("requests.get", side_effect=mock_request_get)
def test_config_source_type(mock_request_get):
    c = Config(get_config_file("socrata")).compute
    assert c.source.socrata
    c = Config(get_config_file("url")).compute
    assert c.source.url


def test_config_version_today():
    c = Config(get_config_file("socrata"))
    version = c.version_today
    assert len(version) == 8  # format: YYYYMMDD
    assert int(version[-2:]) <= 31  # check date
    assert int(version[-4:-2]) <= 12  # check month


@patch("requests.get", side_effect=mock_request_get)
def test_config_compute(mock_request_get):
    dataset = Config(get_config_file("socrata")).compute
    assert dataset.source.gdalpath
    assert dataset.source.options
    assert dataset.source.geometry
    assert dataset.destination.fields == []
    assert dataset.destination.options
    assert dataset.destination.geometry


@patch("requests.get", side_effect=mock_request_get)
def test_config_script(request_get):
    config = Config(f"{template_path}/bpl_libraries.yml").compute
    assert config.source.gdalpath


@patch("dcpy.connectors.esri.arcgis_feature_service.get_layer")
@patch("requests.get", side_effect=mock_request_get)
def test_arcgis_feature_server(request_get, get_layer):
    get_layer.return_value = {}
    config = Config(get_config_file("arcgis_feature_server")).compute
    assert config.source.gdalpath.endswith(f"{config.name}.geojson")


@patch("requests.get", side_effect=mock_request_get)
def test_backwards_compatility_with_jinja_version(request_get):
    config = Config(get_config_file("bpl_libraries_sql_deprecated"))
    computed = config.compute
    assert computed.version == config.version_today


def test_url_with_override_path():
    config = Config(get_config_file("url"), source_path_override=FAKE_PATH)
    dataset = config.compute
    assert dataset.source.url
    assert dataset.source.url.path == FAKE_PATH
    assert dataset.source.gdalpath == f"{FAKE_PATH}/{dataset.source.url.subpath}"


@patch("dcpy.library.script.bpl_libraries.Scriptor.runner")
@patch("dcpy.connectors.esri.arcgis_feature_service.get_layer")
@patch("requests.get", side_effect=mock_request_get)
def test_script_with_override_path(request_get, get_layer, runner):
    runner.return_value = FAKE_PATH
    config = Config(
        get_config_file("bpl_libraries_sql"), source_path_override=FAKE_PATH
    )
    dataset = config.compute
    assert dataset.source.script
    assert dataset.source.script.path == FAKE_PATH
    assert dataset.source.gdalpath == FAKE_PATH


@patch("requests.get", side_effect=mock_request_get)
def test_override_path_failures(request_get):
    with pytest.raises(ValueError, match="Cannot override"):
        Config(get_config_file("socrata"), source_path_override=FAKE_PATH).compute
    with pytest.raises(ValueError, match="Cannot override"):
        Config(
            get_config_file("arcgis_feature_server"), source_path_override=FAKE_PATH
        ).compute
    with pytest.raises(ValueError, match="Cannot override"):
        Config(
            get_config_file("script_no_path"), source_path_override=FAKE_PATH
        ).compute
