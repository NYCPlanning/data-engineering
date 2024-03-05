import yaml

from dcpy.extract import TEMPLATE_DIR, config


def test_validate_all_datasets():
    templates = [t for t in TEMPLATE_DIR.glob("*")]
    assert len(templates) > 0
    for file in templates:
        with open(file, "r") as f:
            s = yaml.safe_load(f)
            val = config.Template(**s)
            assert val
