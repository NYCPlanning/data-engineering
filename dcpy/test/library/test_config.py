import pytest
from unittest.mock import patch
from dcpy.library.config import Config

from . import template_path, get_config_file

FAKE_PATH = "./fake_file.csv"


def test_config_parsed_rendered_template():
    c = Config(get_config_file("url"))
    rendered = c.parsed_rendered_template(version="20v7")
    assert rendered.version == "20v7"


def test_config_source_type():
    c = Config(get_config_file("socrata")).compute
    assert c.source.socrata
    c = Config(get_config_file("url")).compute
    assert c.source.url


def test_config_version_socrata():
    c = Config(get_config_file("socrata"))
    assert c.parsed_unrendered_template.source.socrata
    uid = c.parsed_unrendered_template.source.socrata.uid
    version = c.version_socrata(uid)
    assert len(version) == 8  # format: YYYYMMDD
    assert int(version[-2:]) <= 31  # check date
    assert int(version[-4:-2]) <= 12  # check month


def test_config_version_today():
    c = Config(get_config_file("socrata"))
    version = c.version_today
    assert len(version) == 8  # format: YYYYMMDD
    assert int(version[-2:]) <= 31  # check date
    assert int(version[-4:-2]) <= 12  # check month


def test_config_compute():
    dataset = Config(get_config_file("socrata")).compute
    assert dataset.source.gdalpath
    assert dataset.source.options
    assert dataset.source.geometry
    assert dataset.destination.fields == []
    assert dataset.destination.options
    assert dataset.destination.geometry


def test_config_script():
    config = Config(f"{template_path}/bpl_libraries.yml").compute
    assert True


def test_backwards_compatility_with_jinja_version():
    config = Config(get_config_file("bpl_libraries_sql_deprecated"))
    computed = config.compute
    assert computed.version == config.version_today


def test_url_with_override_path():
    config = Config(get_config_file("url"), path_override=FAKE_PATH)
    dataset = config.compute
    assert dataset.source.url
    assert dataset.source.url.path == FAKE_PATH
    assert dataset.source.gdalpath == f"{FAKE_PATH}/{dataset.source.url.subpath}"


@patch("dcpy.library.script.bpl_libraries.Scriptor.runner")
def test_script_with_override_path(runner):
    runner.return_value = FAKE_PATH
    config = Config(get_config_file("bpl_libraries_sql"), path_override=FAKE_PATH)
    dataset = config.compute
    assert dataset.source.script
    assert dataset.source.script.path == FAKE_PATH
    assert dataset.source.gdalpath == FAKE_PATH


def test_override_path_failures():
    with pytest.raises(ValueError, match="Cannot override"):
        Config(get_config_file("socrata"), path_override=FAKE_PATH).compute
    with pytest.raises(ValueError, match="Cannot override"):
        Config(
            get_config_file("arcgis_feature_server"), path_override=FAKE_PATH
        ).compute
    with pytest.raises(ValueError, match="Cannot override"):
        Config(get_config_file("script_no_path"), path_override=FAKE_PATH).compute
