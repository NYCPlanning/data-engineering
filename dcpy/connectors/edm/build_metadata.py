import os
from datetime import datetime
import pytz

from dcpy.utils import git


def build_name(name: str | None = None) -> str:
    if os.environ.get("BUILD_ENGINE_SCHEMA"):
        return os.environ["BUILD_ENGINE_SCHEMA"]

    name = git.branch() if not name else name
    # DB schema names can't use dashes
    return name.replace("-", "_")


def generate() -> dict[str, str]:
    """Generates "standard" s3 metadata for our files"""
    metadata = {
        "date_created": datetime.now(pytz.timezone("America/New_York")).strftime(
            "%Y-%m-%d %H:%M:%S %z"
        )
    }
    metadata["commit"] = git.commit_hash()
    if os.environ.get("CI"):
        metadata["run_url"] = git.action_url()
    return metadata
