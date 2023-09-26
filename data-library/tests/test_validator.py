from pathlib import Path

import yaml

from library.validator import Validator

with open(f"{Path(__file__).parent}/data/test_none.yml", "r") as f:
    v = Validator(yaml.safe_load(f.read()))


def test_tree_structure():
    assert v.tree_is_valid


def test_dataset_name_matches():
    assert v.dataset_name_matches


def test_has_only_one_source():
    assert v.has_only_one_source
