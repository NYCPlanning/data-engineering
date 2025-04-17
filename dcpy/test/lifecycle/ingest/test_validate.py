from io import BytesIO
import json
import pytest
import yaml

from dcpy.test.conftest import RECIPES_BUCKET
from dcpy.models.lifecycle.ingest import Template
from dcpy.utils import s3
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.ingest.connectors import processed_datastore
from dcpy.lifecycle.ingest import transform, validate

from .shared import (
    TEST_OUTPUT,
    BASIC_CONFIG,
    BASIC_LIBRARY_CONFIG,
    PROD_TEMPLATE_DIR,
)


def test_template_dir_exists():
    """Sanity check. It must exist for test below."""
    assert PROD_TEMPLATE_DIR.exists(), (
        f"Template directory (production) '{PROD_TEMPLATE_DIR}' doesn't exist."
    )


@pytest.mark.parametrize("dataset", [t.name for t in PROD_TEMPLATE_DIR.glob("*")])
def test_validate_all_templates(dataset):
    with open(PROD_TEMPLATE_DIR / dataset, "r") as f:
        s = yaml.safe_load(f)
    template = Template(**s)
    transform.validate_processing_steps(
        template.id, template.ingestion.processing_steps
    )


class TestValidateAgainstExistingVersions:
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
