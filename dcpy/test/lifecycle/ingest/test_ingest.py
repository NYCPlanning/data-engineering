import yaml
from dcpy.models.lifecycle.ingest import Template

from dcpy.lifecycle.ingest import configure, transform


def test_validate_all_datasets():
    templates = [t for t in configure.TEMPLATE_DIR.glob("*")]
    assert len(templates) > 0
    for file in templates:
        with open(file, "r") as f:
            s = yaml.safe_load(f)
        template = Template(**s)
        transform.validate_processing_steps(template.id, template.processing_steps)
