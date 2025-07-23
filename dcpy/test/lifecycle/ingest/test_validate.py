from io import BytesIO
import json
import pytest
import yaml

from dcpy.test.conftest import RECIPES_BUCKET
from dcpy.models.lifecycle.ingest_inputs import Template
from dcpy.utils import s3
from dcpy.connectors.edm import recipes
from dcpy.lifecycle.ingest.connectors import processed_datastore
from dcpy.lifecycle.ingest import transform, validate

from .shared import (
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
