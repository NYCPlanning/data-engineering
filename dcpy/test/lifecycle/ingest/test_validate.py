import pytest
import yaml

from dcpy.models.lifecycle.ingest import Template
from dcpy.lifecycle.ingest import configure, transform


@pytest.mark.parametrize("dataset", [t.name for t in configure.TEMPLATE_DIR.glob("*")])
def test_validate_all_datasets(dataset):
    with open(configure.TEMPLATE_DIR / dataset, "r") as f:
        s = yaml.safe_load(f)
    template = Template(**s)
    transform.validate_processing_steps(
        template.id, template.ingestion.processing_steps
    )
