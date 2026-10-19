from pathlib import Path

import yaml


def read_datasets_yml() -> dict:
    with open(Path(__file__).parent.parent / ("datasets.yml"), "r") as f:
        return yaml.safe_load(f.read())["datasets"]
