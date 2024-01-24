from dcpy.library.config import Config

from . import template_path, get_config_file


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
