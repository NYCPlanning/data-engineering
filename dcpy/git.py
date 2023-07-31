import os
import subprocess

from . import REPO_ROOT_PATH


def git_username():
    if os.environ.get("CI"):
        return os.environ.get("GITHUB_ACTOR_ID", "CI")
    else:
        return (
            subprocess.run(["git", "config", "user.name"], stdout=subprocess.PIPE)
            .stdout.strip()
            .decode()
        )


def git_branch():
    if os.environ.get("CI"):
        ## REF_NAME - for push/workflow_dispatch
        ## HEAD_REF - for pull request triggers
        return os.environ.get("GITHUB_REF_NAME", os.environ.get("GITHUB_HEAD_REF"))
    else:
        return (
            subprocess.run(
                ["git", "rev-parse", "--symbolic-full-name", "--abbrev-ref", "HEAD"],
                stdout=subprocess.PIPE,
            )
            .stdout.strip()
            .decode()
        )
