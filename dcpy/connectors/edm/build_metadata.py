import os
from datetime import datetime
import pytz

from dcpy.utils import git


def build_name(event: str | None = None, source: str | None = None) -> str:
    if os.environ.get("BUILD_ENGINE_SCHEMA"):
        return os.environ["BUILD_ENGINE_SCHEMA"]

    event = git.event_name() if not event else event
    if event == "pull_request":
        prefix = "pr"
    else:
        prefix = "run"

    source = git.branch() if not source else source
    suffix = source.replace("-", "_")
    return f"{prefix}_{suffix}"


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
