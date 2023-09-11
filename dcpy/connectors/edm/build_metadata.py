from datetime import datetime
import pytz

from dcpy.utils import git


def generate() -> dict[str, str]:
    """Generates "standard" s3 metadata for our files"""
    metadata = {
        "date_created": datetime.now(pytz.timezone("America/New_York")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    }
    commit = git.commit_hash()
    if commit:
        metadata["commit"] = commit
    action_url = git.action_url()
    if action_url:
        metadata["run_url"] = action_url
    return metadata
