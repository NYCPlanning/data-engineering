from pathlib import Path

import yaml

from dcpy.library import TEMPLATE_DIR
from dcpy.library.validator import Validator

with open(f"{Path(__file__).parent}/data/test_none.yml", "r") as f:
    v = Validator(yaml.safe_load(f.read()))


def test_tree_structure():
    assert v.tree_is_valid


def test_has_only_one_source():
    assert v.has_only_one_source


def test_validate_all_datasets():
    for file in TEMPLATE_DIR.glob("*"):
        with open(file, "r") as f:
            val = Validator(yaml.safe_load(f))
        assert val.tree_is_valid
        assert v.has_only_one_source
