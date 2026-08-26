#!/usr/bin/env python3
"""
Emit the LDF header's release vars as JSON, for dbt's --vars.

The LDF header names the two LION releases the edition spans and the dates each was
deployed. Nothing in the source data records them, so they live in recipe.yml under
custom.ldf and are read from there rather than typed into the dbt command per release.

Usage:
    dbt build --vars "$(python3 scripts/ldf_vars.py)"

This script is a stopgap. It exists because CSCL invokes `dbt build` as its own workflow
step, so nothing carries recipe values into dbt's process. Products that declare their
build under `stage_config` (green_fast_track, pluto, lift) have dcpy launch dbt, and dcpy
exports recipe `env` into that process. When CSCL moves to `stage_config`, these four
values move from `custom.ldf` to `env`, dbt_project.yml reads them with `env_var()`, and
this script goes away.
"""

import json
from pathlib import Path

from dcpy.lifecycle.builds import plan

REQUIRED = ["previous_version", "old_release_date", "new_release_date"]


def ldf_vars(recipe_path: Path = Path("recipe.yml")) -> dict[str, str]:
    recipe = plan.recipe_from_yaml(recipe_path)
    assert recipe.version, f"{recipe_path} has no version"

    ldf = (recipe.custom or {}).get("ldf", {})
    missing = [key for key in REQUIRED if not ldf.get(key)]
    assert not missing, f"{recipe_path} custom.ldf is missing {', '.join(missing)}"

    return {
        # The LDF writes releases uppercase ('26B'); recipe versions are lowercase.
        "ldf_old_release": str(ldf["previous_version"]).upper(),
        "ldf_old_release_date": str(ldf["old_release_date"]),
        "ldf_new_release": recipe.version.upper(),
        "ldf_new_release_date": str(ldf["new_release_date"]),
    }


if __name__ == "__main__":
    print(json.dumps(ldf_vars()))
