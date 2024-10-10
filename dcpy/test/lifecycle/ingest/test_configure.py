from datetime import datetime
from pathlib import Path
import pytest
from unittest import mock, TestCase

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.models import file
from dcpy.models.connectors import socrata, web
from dcpy.models.lifecycle.ingest import LocalFileSource
from dcpy.utils import s3
from dcpy.lifecycle.ingest import configure

from dcpy.test.conftest import mock_request_get
from . import RESOURCES, TEST_DATASET_NAME, Sources, SOURCE_FILENAMES, TEMPLATE_DIR


def test_jinja_vars():
    no_vars = configure.get_jinja_vars("fake_yml: value")
    assert len(no_vars) == 0, "No variables should have been found"
    vars = configure.get_jinja_vars(r"fake_yml: {{ version }}")
    assert vars == {"version"}, "One var, 'version', should have been found"


class TestReadTemplate:
    """
    Tests configure.read_template
    In addition to ensuring templates are parsed correctly, catches specific errors around jinja templating
    """

    def test_simple(self):
        template = configure.read_template("bpl_libraries", template_dir=TEMPLATE_DIR)
        assert isinstance(template.ingestion.source, web.GenericApiSource)
        assert isinstance(
            template.ingestion.file_format,
            file.Json,
        )

    def test_jinja(self):
        template = configure.read_template(
            "dcp_atomicpolygons", version="test", template_dir=TEMPLATE_DIR
        )
        assert isinstance(template.ingestion.source, web.FileDownloadSource)
        assert isinstance(
            template.ingestion.file_format,
            file.Shapefile,
        )

    def test_invalid_jinja(self):
        with pytest.raises(
            Exception,
            match="'version' is only suppored jinja var. Vars in template: ",
        ):
            configure.read_template(
                "invalid_jinja", version="dummy", template_dir=RESOURCES
            )


class TestGetVersion(TestCase):
    @mock.patch("requests.get", side_effect=mock_request_get)
    def test_socrata(self, get):
        source = socrata.Source(
            type="socrata", org="nyc", uid="w7w3-xahh", format="csv"
        )
        ### based on mocked response in dcpy/test/conftest.py
        assert configure.get_version(source) == "20240412"

    @pytest.mark.usefixtures("create_buckets")
    def test_gis_dataset(self):
        datestring = "20240412"
        s3.client().put_object(
            Bucket=PUBLISHING_BUCKET,
            Key=f"datasets/{TEST_DATASET_NAME}/{datestring}/{TEST_DATASET_NAME}.zip",
        )
        assert configure.get_version(Sources.gis) == datestring

    def test_rely_on_timestamp(self):
        timestamp = datetime.today()
        source = LocalFileSource(type="local_file", path=Path("."))
        assert configure.get_version(source, timestamp) == timestamp.strftime("%Y%m%d")

    def test_rely_on_timestamp_fails(self):
        with pytest.raises(TypeError, match="Version cannot be dynamically determined"):
            configure.get_version(LocalFileSource(type="local_file", path=Path(".")))


@pytest.mark.parametrize(["source", "expected"], SOURCE_FILENAMES)
def test_get_filename(source, expected):
    configure.get_filename(source, TEST_DATASET_NAME) == expected


def test_get_filename_invalid_source():
    with pytest.raises(NotImplementedError, match="Source type"):
        configure.get_filename(None, TEST_DATASET_NAME)


class TestGetConfig:
    def test_standard_no_geom(self):
        config = configure.get_config("dob_now_permits", template_dir=TEMPLATE_DIR)
        # ensure no reprojection or multi step (no target_crs, no geom respectively)
        # ensure default 'clean_column_names' step is added
        assert len(config.ingestion.processing_steps) == 1
        assert config.ingestion.processing_steps[0].name == "clean_column_names"

    def test_standard(self):
        config = configure.get_config(
            "dca_operatingbusinesses", template_dir=TEMPLATE_DIR
        )
        # ensure no reprojection
        # ensure default 'clean_column_names' and multi steps are added
        assert len(config.ingestion.processing_steps) == 2
        assert config.ingestion.processing_steps[0].name == "clean_column_names"

    def test_clean_column_names_defined(self):
        config = configure.get_config("bpl_libraries", template_dir=TEMPLATE_DIR)
        # ensure no reprojection step
        # ensure default 'clean_column_names' and 'multi' steps are added
        assert len(config.ingestion.processing_steps) == 2
        assert config.ingestion.processing_steps[0].name == "clean_column_names"
        assert config.ingestion.processing_steps[0].args == {"replace": {"data.": ""}}

    def test_reproject(self):
        config = configure.get_config(
            "dcp_addresspoints", version="24c", template_dir=TEMPLATE_DIR
        )
        assert len(config.ingestion.processing_steps) == 3
        assert config.ingestion.processing_steps[0].name == "reproject"

    def test_no_mode(self):
        standard = configure.get_config(
            "dcp_pop_acs2010_demographic", version="test", template_dir=TEMPLATE_DIR
        )
        assert standard.ingestion.processing_steps
        assert "append_prev" not in [
            s.name for s in standard.ingestion.processing_steps
        ]

    def test_mode(self):
        append = configure.get_config(
            "dcp_pop_acs2010_demographic",
            version="test",
            mode="append",
            template_dir=TEMPLATE_DIR,
        )
        assert "append_prev" in [s.name for s in append.ingestion.processing_steps]

    def test_invalid_mode(self):
        with pytest.raises(ValueError):
            configure.get_config(
                "dcp_pop_acs2010_demographic",
                version="test",
                mode="fake_mode",
                template_dir=TEMPLATE_DIR,
            )
