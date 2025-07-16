import os
import subprocess


def event_name() -> str:
    if os.environ.get("CI"):
        return os.environ["GITHUB_EVENT_NAME"]
    return "local"


def username() -> str:
    if event_name() == "local":
        return (
            subprocess.run(["git", "config", "user.name"], stdout=subprocess.PIPE)
            .stdout.strip()
            .decode()
        )
    return os.environ.get("GITHUB_ACTOR_ID", "CI")


def branch() -> str:
    if event_name() == "local":
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
    elif event_name() == "pull_request":
        return os.environ["GITHUB_HEAD_REF"]
    else:
        return os.environ["GITHUB_REF_NAME"]


def commit_hash() -> str:
    if event_name() == "local":
        return (
            subprocess.run(["git", "rev-parse", "HEAD"], stdout=subprocess.PIPE)
            .stdout.decode("ascii")
            .strip()
        )
    return os.environ["GITHUB_SHA"]


def action_url() -> str:
    if event_name() == "local":
        return f"local_{branch}_{username}"
    return f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
