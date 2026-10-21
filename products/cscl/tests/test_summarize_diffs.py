"""summarize_diffs: export group labels come from item-level comments only."""

import os
import sys
from pathlib import Path

os.environ.setdefault("VALIDATION_DIR", "/tmp")
sys.path.insert(0, str(Path(__file__).parent.parent / "poc_validation"))

from summarize_diffs import export_name_to_group_from_text  # noqa: E402

RECIPE = """\
datasets:
  - name: not_an_export
exports:
  zip_name: output
  datasets:
  # GeoSupport LION
  - name: lion_a
    # inside an entry
    filename: a.dat
      # deeper still
  - name: lion_b
  # SAF
  - name: saf_a

  # a note about arcs that
  # continues here
  # District boundary geodatabase
  # filename must match prod's
  - name: dist_a
  - name: dist_b
"""


def test_only_item_level_comments_are_groups():
    assert export_name_to_group_from_text(RECIPE) == {
        "lion_a": "GeoSupport LION",
        "lion_b": "GeoSupport LION",
        "saf_a": "SAF",
        "dist_a": "District boundary geodatabase",
        "dist_b": "District boundary geodatabase",
    }
