import os
import subprocess
from typing import Optional


def username() -> str:
    if os.environ.get("CI"):
        return os.environ.get("GITHUB_ACTOR_ID", "CI")
    else:
        return (
            subprocess.run(["git", "config", "user.name"], stdout=subprocess.PIPE)
            .stdout.strip()
            .decode()
        )


def branch() -> str:
    if os.environ.get("CI"):
        ## REF_NAME - for push/workflow_dispatch
        ## HEAD_REF - for pull request triggers
        return os.environ.get("GITHUB_REF_NAME", os.environ["GITHUB_HEAD_REF"])
    else:
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


def commit_hash() -> str:
    if os.environ.get("CI"):
        return os.environ["GITHUB_SHA"]
    else:
        return (
            subprocess.run(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE)
            .stdout.decode("ascii")
            .strip()
        )


def action_url() -> str:
    return f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
