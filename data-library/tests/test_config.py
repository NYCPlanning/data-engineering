from pathlib import Path
from library.config import Config

from . import template_path, get_config_file


def test_config_parsed_rendered_template():
    c = Config(get_config_file("url"))
    rendered = c.parsed_rendered_template(version="20v7")
    assert rendered["dataset"]["version"] == "20v7"


def test_config_source_type():
    c = Config(get_config_file("socrata"))
    assert c.source_type == "socrata"
    c = Config(get_config_file("url"))
    assert c.source_type == "url"


def test_config_version_socrata():
    c = Config(get_config_file("socrata"))
    uid = c.parsed_unrendered_template["dataset"]["source"]["socrata"]["uid"]
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
    config = Config(get_config_file("socrata")).compute
    assert type(config["dataset"]["source"]["url"]) == dict


def test_config_compute_parsed():
    dataset, source, destination, info = Config(
        get_config_file("socrata")
    ).compute_parsed
    assert dataset["source"] == source
    assert dataset["info"] == info
    assert dataset["destination"] == destination
    assert "url" in list(source.keys())
    assert "options" in list(source.keys())
    assert "geometry" in list(source.keys())
    assert "fields" in list(destination.keys())
    assert "options" in list(destination.keys())
    assert "geometry" in list(destination.keys())


def test_config_script():
    config = Config(f"{template_path}/bpl_libraries.yml").compute
    assert True
