import os

from dcpy.utils import git


def build_name(name: str | None = None) -> str:
    if os.environ.get("BUILD_ENGINE_SCHEMA"):
        return os.environ["BUILD_ENGINE_SCHEMA"]

    name = git.branch() if not name else name
    # DB schema names can't use dashes
    return name.replace("-", "_")
