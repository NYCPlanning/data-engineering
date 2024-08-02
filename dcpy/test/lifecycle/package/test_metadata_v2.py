from pathlib import Path

import dcpy.models.product.dataset.metadata_v2 as md

METADATA_PATH = (
    Path(__file__).parent.resolve() / "resources" / "metadata" / "metadata_v2.yml"
)
VERSION = "24b"


def _get_md():
    return md.Metadata.from_path(METADATA_PATH, template_vars={"version": VERSION})


def test_parse_metadata():
    _get_md()
