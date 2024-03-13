from pathlib import Path
import pytest
import yaml

from dcpy.models.lifecycle.extract import Template, ToParquetMeta
from dcpy.models.connectors import web
from dcpy.lifecycle.extract import TEMPLATE_DIR, metadata

RESOURCES = Path(__file__).parent / "resources"


def test_validate_all_datasets():
    templates = [t for t in TEMPLATE_DIR.glob("*")]
    assert len(templates) > 0
    for file in templates:
        with open(file, "r") as f:
            s = yaml.safe_load(f)
        val = Template(**s)
        assert val


def test_jinja_vars():
    no_vars = metadata.get_jinja_vars("fake_yml: value")
    assert len(no_vars) == 0, "No variables should have been found"
    vars = metadata.get_jinja_vars(r"fake_yml: {{ version }}")
    assert vars == {"version"}, "One var, 'version', should have been found"


def test_read_template():
    with pytest.raises(
        Exception,
        match="Version must be supplied explicitly to be rendered in template",
    ):
        metadata.read_template("dcp_atomicpolygons")

    with pytest.raises(
        Exception,
        match="'version' is only suppored jinja var. Unsupported vars in template: ",
    ):
        metadata.read_template("invalid_jinja", version="dummy", template_dir=RESOURCES)

    template = metadata.read_template("dcp_atomicpolygons", version="test")
    assert isinstance(template.source, web.FileDownloadSource)
    assert (
        template.source.url
        == "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nyap_test.zip"
    )
    assert isinstance(
        template.transform_to_parquet_metadata,
        ToParquetMeta.Shapefile,
    )
