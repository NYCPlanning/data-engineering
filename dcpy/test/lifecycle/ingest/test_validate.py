from io import BytesIO
import json
import pytest
from pathlib import Path

from dcpy.test.conftest import RECIPES_BUCKET
from dcpy.utils import s3
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.ingest.connectors import processed_datastore
from dcpy.lifecycle.ingest import validate

from .shared import (
    TEST_OUTPUT,
    BASIC_CONFIG,
    BASIC_LIBRARY_CONFIG,
)


class TestValidateAgainstExistingVersion:
    def test_existing_library(self, create_buckets):
        ds = BASIC_LIBRARY_CONFIG.sparse_dataset
        config_str = json.dumps(BASIC_LIBRARY_CONFIG.model_dump(mode="json"))
        s3.upload_file_obj(
            BytesIO(config_str.encode()),
            RECIPES_BUCKET,
            f"{recipes.s3_folder_path(ds)}/config.json",
            BASIC_LIBRARY_CONFIG.dataset.acl,
        )
        assert processed_datastore.version_exists(ds.id, ds.version)
        validate.validate_against_existing_version(ds.id, ds.version, TEST_OUTPUT)

    def test_existing(self, create_buckets):
        ds = BASIC_CONFIG.dataset
        recipes.archive_dataset(BASIC_CONFIG, TEST_OUTPUT, acl="private")
        assert processed_datastore.version_exists(ds.id, ds.version)
        validate.validate_against_existing_version(ds.id, ds.version, TEST_OUTPUT)

    def test_existing_data_diffs(self, create_buckets):
        ds = BASIC_CONFIG.dataset
        recipes.archive_dataset(BASIC_CONFIG, TEST_OUTPUT, acl="private")
        assert processed_datastore.version_exists(ds.id, ds.version)
        with pytest.raises(FileExistsError):
            validate.validate_against_existing_version(
                ds.id, ds.version, TEST_OUTPUT.parent / "test.parquet"
            )


INGEST_TEMPLATES = Path(__file__).parent / "resources" / "templates"


def test_validate_template_file_valid():
    """Test validation of a valid template file."""
    valid_file = INGEST_TEMPLATES / "dcp_addresspoints.yml"
    validate.validate_template_file(valid_file)


def test_validate_template_file_invalid():
    """Test validation of an invalid template file."""
    invalid_file = INGEST_TEMPLATES / "invalid_template.yml"
    with pytest.raises(Exception):
        validate.validate_template_file(invalid_file)


def test_validate_template_folder():
    """Test validation of the ingest_templates folder."""
    errors = validate.validate_template_folder(INGEST_TEMPLATES)
    # hypothetically an invalid "invalid_template.yaml" in the folder
    assert len(errors) == 1
    assert "invalid_template.yml" in errors


def test_validate_template_folder_raise_on_error():
    """Test validation with raise_on_error=True."""
    with pytest.raises(ValueError, match="Validation failed"):
        validate.validate_template_folder(INGEST_TEMPLATES, raise_on_error=True)
