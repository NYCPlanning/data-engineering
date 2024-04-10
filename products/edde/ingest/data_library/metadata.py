from os import makedirs, path
from pathlib import Path

import yaml

metadata = {"datasets": []}


class MyDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)


def add_version(dataset: str, version: str):
    if version.isnumeric():
        version = int(version)
    metadata["datasets"].append({"name": dataset, "version": version})


def dump_metadata(category: str):
    folder_path = f".staging/{category}"
    if not path.exists(folder_path):
        makedirs(folder_path)
    with open(
        Path(__file__).parent.parent.parent / f"{folder_path}/metadata.yml", "w"
    ) as outfile:
        yaml.dump(metadata, outfile, Dumper=MyDumper, default_flow_style=False)
