"""Turn a map config pulled with `carto maps get` into a bundle `carto maps create` accepts.

A new map can't reuse another map's dataset ids, so each dataset id becomes a `$ref` name
(from its label) and every reference to it becomes "$ref:<name>".

Usage: python to_create_bundle.py lift_tracker_dev.json "New map title" > new_map.json
"""

import json
import re
import sys

config_path, title = sys.argv[1], sys.argv[2]
with open(config_path) as f:
    config = json.load(f)

for key in ("id", "containerId", "agent"):
    config.pop(key, None)
config["title"] = title

text = json.dumps(config)
for dataset in config["datasets"]:
    ref = re.sub(r"[^a-z0-9]+", "_", dataset["label"].lower()).strip("_")
    text = text.replace(f'"id": "{dataset["id"]}"', f'"$ref": "{ref}"')
    text = text.replace(f'"{dataset["id"]}"', f'"$ref:{ref}"')

print(json.dumps(json.loads(text), indent=2))
