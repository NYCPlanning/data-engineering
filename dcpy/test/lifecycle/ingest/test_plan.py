from datetime import datetime
from pathlib import Path
import pytest
from unittest import mock
from pydantic import BaseModel

from dcpy.configuration import RECIPES_BUCKET, PUBLISHING_BUCKET
from dcpy.models import file
from dcpy.models.lifecycle.ingest import Source
from dcpy.utils import s3
from dcpy.lifecycle.ingest import plan

from dcpy.test.conftest import mock_request_get
from .shared import (
    RESOURCES,
    TEST_DATASET_NAME,
    Sources,
    INGEST_DEF_DIR,
)


def test_jinja_vars():
    no_vars = plan.get_jinja_vars("fake_yml: value")
    assert len(no_vars) == 0, "No variables should have been found"
    vars = plan.get_jinja_vars(r"fake_yml: {{ version }}")
    assert vars == {"version"}, "One var, 'version', should have been found"


class TestReadTemplate:
    """
    Tests configure.read_definition
    In addition to ensuring definitions are parsed correctly, catches specific errors around jinja templating
    """

    def test_simple(self):
        definition = plan.read_definition(
            "bpl_libraries", definition_dir=INGEST_DEF_DIR
        )
        assert isinstance(
            definition.ingestion.file_format,
            file.Json,
        )

    def test_jinja(self):
        definition = plan.read_definition(
            "dcp_atomicpolygons", version="test", definition_dir=INGEST_DEF_DIR
        )
        assert isinstance(
            definition.ingestion.file_format,
            file.Shapefile,
        )

    def test_invalid_jinja(self):
        with pytest.raises(
            Exception,
            match="'version' is only suppored jinja var. Vars in definition: ",
        ):
            plan.read_definition(
                "invalid_jinja", version="dummy", definition_dir=RESOURCES
            )


class SparseBuildMetadata(BaseModel):
    version: str


class TestGetVersion:
    @mock.patch("requests.get", side_effect=mock_request_get)
    def test_socrata(self, get):
        ### based on mocked response in dcpy/test/conftest.py
        assert plan.get_version(Sources.socrata, None) == "20240412"

    @mock.patch("requests.get", side_effect=mock_request_get)
    def test_esri(self, get):
        ### based on mocked response in dcpy/test/conftest.py
        assert plan.get_version(Sources.esri, None) == "20240806"

    def test_s3(self, create_buckets):
        timestamp = datetime.today()
        version = timestamp.strftime("%Y%m%d")
        s3.client().put_object(
            Bucket=RECIPES_BUCKET,
            Key=f"datasets/{TEST_DATASET_NAME}/{version}/{TEST_DATASET_NAME}.zip",
        )
        assert plan.get_version(Sources.s3, timestamp) == version

    def test_gis_dataset(self, create_buckets):
        datestring = "20240412"
        s3.client().put_object(
            Bucket=PUBLISHING_BUCKET,
            Key=f"datasets/{TEST_DATASET_NAME}/{datestring}/{TEST_DATASET_NAME}.zip",
        )
        assert plan.get_version(Sources.gis, None) == datestring

    @mock.patch("dcpy.connectors.edm.publishing.BuildMetadata", SparseBuildMetadata)
    def test_de_publishing(self, create_buckets):
        datestring = "20240412"
        s3.client().put_object(
            Bucket=PUBLISHING_BUCKET,
            Key=f"{TEST_DATASET_NAME}/publish/{datestring}/build_metadata.json",
            Body=f"{{'version': '{datestring}'}}".encode(),
        )
        assert plan.get_version(Sources.de_publish, None) == datestring

    def test_rely_on_timestamp(self):
        timestamp = datetime.today()
        source = Source(type="local_file", key=".")
        assert plan.get_version(source, timestamp) == timestamp.strftime("%Y%m%d")


class TestGetConfig:
    """Tests both get_config and determine_processing_steps"""

    def test_reproject(self):
        config = plan.get_config(
            "dcp_addresspoints", version="24c", definition_dir=INGEST_DEF_DIR
        )
        assert len(config.ingestion.processing_steps) == 1
        assert config.ingestion.processing_steps[0].name == "reproject"

    def test_no_mode(self):
        standard = plan.get_config(
            "dcp_pop_acs2010_demographic", version="test", definition_dir=INGEST_DEF_DIR
        )
        assert standard.ingestion.processing_steps
        assert "append_prev" not in [
            s.name for s in standard.ingestion.processing_steps
        ]

    def test_mode(self):
        append = plan.get_config(
            "dcp_pop_acs2010_demographic",
            version="test",
            mode="append",
            definition_dir=INGEST_DEF_DIR,
        )
        assert "append_prev" in [s.name for s in append.ingestion.processing_steps]

    def test_invalid_mode(self):
        with pytest.raises(ValueError):
            plan.get_config(
                "dcp_pop_acs2010_demographic",
                version="test",
                mode="fake_mode",
                definition_dir=INGEST_DEF_DIR,
            )

    def test_file_path_override(self):
        file_path = Path("dir/fake_file_path")
        config = plan.get_config(
            "dcp_addresspoints",
            version="24c",
            definition_dir=INGEST_DEF_DIR,
            local_file_path=file_path,
        )
        assert config.ingestion.source == Source(type="local_file", key=str(file_path))
