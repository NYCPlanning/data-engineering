from __future__ import annotations

import os
import subprocess
from datetime import datetime
from typing import Literal

import pytz
from pydantic import BaseModel, field_serializer


class CIRun(BaseModel):
    dispatch_event: str
    url: str
    job: str | None = None


class User(BaseModel, extra="forbid"):
    username: str


class RunDetails(BaseModel):
    type: Literal["manual", "ci"]
    runner: CIRun | User
    timestamp: datetime
    logged: bool = False

    def __init__(self, **kwargs):
        if "runner" not in kwargs:
            if "type" in kwargs and kwargs["type"] == "ci":
                kwargs["runner"] = CIRun(**kwargs)
            elif "user" in kwargs:
                kwargs["runner"] = User(username=kwargs["user"])
            else:
                raise Exception(
                    f"Unexpected format of run/execution details:\n{kwargs}"
                )
        super().__init__(**kwargs)

    @field_serializer("timestamp")
    def serialize_timestamp(self, timestamp: datetime, _info) -> str:
        return timestamp.isoformat()

    @property
    def runner_string(self) -> str:
        match self.runner:
            case CIRun() as ci:
                runner = ci.url
            case User() as user:
                if user.username:
                    runner = f"Manual - {user.username}"
                else:
                    runner = "Manual"
        return runner


def get_run_details() -> RunDetails:
    def try_func(func):
        try:
            return func()
        except Exception:
            return "could not parse"

    timestamp = datetime.now(pytz.timezone("America/New_York"))

    if os.environ.get("CI"):
        type = "ci"
        runner: CIRun | User = CIRun(
            dispatch_event=os.environ.get("GITHUB_EVENT_NAME", "could not parse"),
            url=try_func(
                lambda: f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
            ),
            job=os.environ.get("GITHUB_JOB", "could not parse"),
        )
    else:
        type = "manual"
        git_user = try_func(
            lambda: subprocess.run(
                ["git", "config", "user.name"], stdout=subprocess.PIPE
            )
            .stdout.strip()
            .decode()
        )
        runner = User(username=git_user)

    return RunDetails(type=type, runner=runner, timestamp=timestamp)
