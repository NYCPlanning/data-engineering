from io import BytesIO
import json
import pytest
from pathlib import Path

from dcpy.models import library
from dcpy.test.conftest import RECIPES_BUCKET
from dcpy.utils import s3
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.ingest.connectors import processed_datastore
from dcpy.lifecycle.ingest import validate

from .shared import (
    TEST_OUTPUT,
    RUN_DETAILS,
    INGEST_DEF_DIR,
    TEST_DATASET_NAME,
    DOWNSTREAM_DATASET_1,
)

# TODO put this in a file
BASIC_LIBRARY_CONFIG = library.Config(
    dataset=library.DatasetDefinition(
        name=TEST_DATASET_NAME,
        version="20240926",
        acl="public-read",
        source=library.DatasetDefinition.SourceSection(),
        destination=library.DatasetDefinition.DestinationSection(
            geometry=library.GeometryType(SRS="NONE", type="NONE")
        ),
    ),
    execution_details=RUN_DETAILS,
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
        ds = DOWNSTREAM_DATASET_1.dataset
        recipes.archive_dataset(DOWNSTREAM_DATASET_1, TEST_OUTPUT, acl="private")
        assert processed_datastore.version_exists(ds.id, ds.version)
        validate.validate_against_existing_version(ds.id, ds.version, TEST_OUTPUT)

    def test_existing_data_diffs(self, create_buckets):
        ds = DOWNSTREAM_DATASET_1.dataset
        recipes.archive_dataset(DOWNSTREAM_DATASET_1, TEST_OUTPUT, acl="private")
        assert processed_datastore.version_exists(ds.id, ds.version)
        with pytest.raises(FileExistsError):
            validate.validate_against_existing_version(
                ds.id, ds.version, TEST_OUTPUT.parent / "test.parquet"
            )


@pytest.mark.parametrize("ds_id", ["simple", "one_to_many"])
def test_validate_definition_file_valid(ds_id):
    """Test validation of a valid definition file."""
    valid_file = INGEST_DEF_DIR / f"{ds_id}.yml"
    validate.validate_definition_file(valid_file)


def test_validate_definition_file_invalid():
    """Test validation of an invalid definition file."""
    invalid_file = INGEST_DEF_DIR / "invalid_model.yml"
    with pytest.raises(Exception):
        validate.validate_definition_file(invalid_file)


def test_validate_definition_folder():
    """Test validation of the ingest_definitions folder."""
    errors = validate.validate_definition_folder(INGEST_DEF_DIR)
    # hypothetically an invalid "invalid_definition.yml" in the folder
    assert len(errors) == 2
    assert set(errors.keys()) == {
        "invalid_model.yml",
        "invalid_jinja.yml",
    }


def test_validate_definition_folder_nonexistent():
    """Test validation of a non-existent folder."""
    with pytest.raises(FileNotFoundError):
        validate.validate_definition_folder(Path("nonexistent"))
