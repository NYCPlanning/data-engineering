import yaml

from dcpy.extract import TEMPLATE_DIR, models


def test_validate_all_datasets():
    templates = [t for t in TEMPLATE_DIR.glob("*")]
    assert len(templates) > 0
    for file in templates:
        with open(file, "r") as f:
            s = yaml.safe_load(f)
            val = models.ImportDefinition(**s)
            assert val
