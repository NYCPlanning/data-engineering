from datetime import datetime
import os
from pathlib import Path
from pydantic import BaseModel
import pytz
import json

from dcpy.utils import git
from dcpy.utils.logging import logger
from dcpy.builds.plan import Recipe


class BuildMetadata(BaseModel, extra="forbid"):
    timestamp: datetime
    commit: str | None = None
    run_url: str | None = None
    version: str
    recipe: Recipe

    def __init__(self, **data):
        if "version" not in data:
            recipe = data["recipe"]
            if recipe.version is not None:
                data["version"] = recipe.version
        super().__init__(**data)

    def dump(self):
        json = self.model_dump(exclude_none=True)
        json["timestamp"] = self.timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")
        return json


def build_name(name: str | None = None) -> str:
    if os.environ.get("BUILD_ENGINE_SCHEMA"):
        return os.environ["BUILD_ENGINE_SCHEMA"]

    name = git.branch() if not name else name
    # DB schema names can't use dashes
    return name.lower().replace("-", "_")


def write_build_metadata(recipe: Recipe, output_folder: Path):
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
        timestamp=timestamp, commit=commit, run_url=run_url, recipe=recipe
    )
    with open(output_file, "w", encoding="utf-8") as f:
        logger.info(f"Writing build metadata to {str(output_file.absolute())}")
        json.dump(build_metadata.dump(), f, indent=4)
