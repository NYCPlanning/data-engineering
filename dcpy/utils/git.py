import os
import subprocess
from typing import Optional


def username() -> Optional[str]:
    if os.environ.get("CI"):
        return os.environ.get("GITHUB_ACTOR_ID", "CI")
    else:
        try:
            return (
                subprocess.run(["git", "config", "user.name"], stdout=subprocess.PIPE)
                .stdout.strip()
                .decode()
            )
        except:
            return None


def branch() -> Optional[str]:
    if os.environ.get("CI"):
        ## REF_NAME - for push/workflow_dispatch
        ## HEAD_REF - for pull request triggers
        return os.environ.get("GITHUB_REF_NAME", os.environ.get("GITHUB_HEAD_REF"))
    else:
        try:
            return (
                subprocess.run(
                    [
                        "git",
                        "rev-parse",
                        "--symbolic-full-name",
                        "--abbrev-ref",
                        "HEAD",
                    ],
                    stdout=subprocess.PIPE,
                )
                .stdout.strip()
                .decode()
            )
        except:
            return None


def commit_hash() -> Optional[str]:
    if os.environ.get("CI"):
        return os.environ.get("GITHUB_SHA")
    else:
        try:
            return (
                subprocess.run(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE)
                .stdout.decode("ascii")
                .strip()
            )
        except:
            return None


def action_url() -> Optional[str]:
    if os.environ.get("CI"):
        return f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    else:
        return None
