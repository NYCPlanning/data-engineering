import json
import os
from datetime import datetime
from pathlib import Path

import pytz

from dcpy.lifecycle.builds.models import BuildMetadata, LoadResult, Recipe
from dcpy.utils import git
from dcpy.utils.logging import logger


def schema_name(name: str) -> str:
    # DB schema names can't use dashes
    return name.lower().replace("-", "_")


def tests_schema_name(build_schema: str) -> str:
    return build_schema + "__tests"


def build_name(name: str | None = None) -> str:
    """The current build's schema. BUILD_ENGINE_SCHEMA wins over `name`, so use
    `schema_name` for another build's schema."""
    if os.environ.get("BUILD_ENGINE_SCHEMA"):
        return os.environ["BUILD_ENGINE_SCHEMA"]
    return schema_name(name or git.branch())


def build_tests_name(build_name: str) -> str:
    if os.environ.get("BUILD_ENGINE_SCHEMA_TESTS"):
        return os.environ["BUILD_ENGINE_SCHEMA_TESTS"]
    return tests_schema_name(build_name)


def write_build_metadata(
    recipe: Recipe, output_folder: Path, load_result: LoadResult | None = None
):
    if not output_folder.exists():
        output_folder.mkdir(parents=True, exist_ok=True)
    output_file = output_folder / "build_metadata.json"
    timestamp = datetime.now(pytz.timezone("America/New_York"))
    try:
        commit = git.commit_hash()
    except Exception:
        commit = None
    if os.environ.get("CI"):
        run_url = git.action_url()
    else:
        run_url = None
    build_metadata = BuildMetadata(
        timestamp=timestamp,
        commit=commit,
        run_url=run_url,
        recipe=recipe,
        load_result=load_result,
    )
    with open(output_file, "w", encoding="utf-8") as f:
        logger.info(f"Writing build metadata to {str(output_file.absolute())}")
        json.dump(build_metadata.model_dump(mode="json"), f, indent=4)
