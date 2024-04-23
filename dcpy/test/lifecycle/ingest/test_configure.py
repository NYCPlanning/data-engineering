from datetime import datetime
from pydantic import TypeAdapter
import pytest
from unittest import mock
import yaml

from dcpy.models import file
from dcpy.models.connectors.edm.publishing import GisDataset
from dcpy.models.connectors import socrata, web
from dcpy.models.lifecycle.ingest import (
    LocalFileSource,
    ScriptSource,
    Source,
)
from dcpy.utils import s3
from dcpy.connectors.edm import publishing
from dcpy.lifecycle.ingest import configure

from dcpy.test.conftest import mock_request_get
from . import RESOURCES


def test_jinja_vars():
    no_vars = configure.get_jinja_vars("fake_yml: value")
    assert len(no_vars) == 0, "No variables should have been found"
    vars = configure.get_jinja_vars(r"fake_yml: {{ version }}")
    assert vars == {"version"}, "One var, 'version', should have been found"


def test_read_template():
    """
    Tests configure.read_template
    In addition to ensuring templates are parsed correctly, catches specific errors around jinja templating
    """
    with pytest.raises(
        Exception,
        match="'version' is only suppored jinja var. Unsupported vars in template: ",
    ):
        configure.read_template(
            "invalid_jinja", version="dummy", template_dir=RESOURCES
        )

    template = configure.read_template("dcp_atomicpolygons", version="test")
    assert isinstance(template.source, web.FileDownloadSource)
    assert (
        template.source.url
        == "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nyap_test.zip"
    )
    assert isinstance(
        template.file_format,
        file.Shapefile,
    )


@mock.patch("requests.get", side_effect=mock_request_get)
def test_get_version(mock_request_get, create_buckets):
    datestring = datetime.today().strftime("%Y%m%d")
    s3.client().put_object(
        Bucket=publishing.BUCKET,
        Key=f"datasets/dcp_borough_boundary/{datestring}/dcp_borough_boundary.zip",
    )
    s3.client().put_object(
        Bucket=publishing.BUCKET,
        Key=f"datasets/dcp_borough_boundary/staging/dcp_borough_boundary.zip",
    )
    with open(RESOURCES / "sources.yml") as f:
        sources = TypeAdapter(list[Source]).validate_python(yaml.safe_load(f))
    for source in sources:
        match source:
            case socrata.Source():
                assert configure.get_version(source) == "20240412"
            case GisDataset():
                assert configure.get_version(source) == datestring
            case LocalFileSource():
                pass
            case ScriptSource():
                pass
            case web.FileDownloadSource():
                pass
            case web.GenericApiSource():
                pass
            case _:
                raise NotImplementedError(
                    f"Source type {source} has not had test set up"
                )


def test_get_filename():
    """
    Tests configure.get_filename for source objects in resources/sources.yml
    Feeds in 'test' as a fake dataset name
    Assumes order in yml file is consistent with order of expected filenames here
    """
    expected_filenames = [
        "test.json",
        "test.csv",
        "dcp_borough_boundary.zip",
        "pad_24a.zip",
        "tmp.txt",
        "rows.csv",
    ]
    with open(RESOURCES / "sources.yml") as f:
        sources = TypeAdapter(list[Source]).validate_python(yaml.safe_load(f))
    for i, source in enumerate(sources):
        assert configure.get_filename(source, "test") == expected_filenames[i]


def test_get_config():
    """
    Tests that configure.get_config runs without exception
    Given other unit tests, mainly confirms that template is correctly converted to config pydantic class
    """
    configure.get_config("dcp_atomicpolygons", version="test")
    assert True
