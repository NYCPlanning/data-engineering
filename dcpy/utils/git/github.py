from __future__ import annotations
import os
import requests
import shutil
from pathlib import Path
from git import Repo
from datetime import datetime
from dateutil.parser import parse as datetime_parse
from typing import List, Dict, Any
from dataclasses import dataclass

ORG = "NYCPlanning"
PERSONAL_TOKEN = os.environ["GHP_TOKEN"]
headers = {"Authorization": "Bearer %s" % PERSONAL_TOKEN}
BASE_URL = f"https://api.github.com/repos/{ORG}"


@dataclass(frozen=True)
class WorkflowRun:
    name: str
    status: str
    conclusion: str
    timestamp: datetime
    url: str

    @classmethod
    def parse_from_github(cls, workflow: dict) -> WorkflowRun:
        """parses json from GitHub into typed class with just what we're interested in"""
        return WorkflowRun(
            name=workflow["name"],
            status=workflow["status"],
            conclusion=workflow["conclusion"],
            timestamp=datetime_parse(workflow["updated_at"], fuzzy=True),
            url=workflow["html_url"],
        )

    @property
    def is_running(self) -> bool:
        return self.status in ["queued", "in_progress"]


def clone_repo(repo: str, output_directory: Path, *, branch: str | None = None) -> Path:
    output_path = output_directory / repo
    if not branch:
        branch = get_default_branch(repo)
    url = f"https://github.com/NYCPlanning/{repo}"
    if output_path.exists():
        shutil.rmtree(output_path)
    Repo.clone_from(url, output_path)
    return output_path


def get_default_branch(repo: str) -> str:
    url = f"https://api.github.com/repos/nycplanning/{repo}"
    response = requests.get(url).json()
    return response["default_branch"]


def get_branches(repo: str, branches_blacklist: List[str] | None = None):
    url = f"https://api.github.com/repos/nycplanning/{repo}/branches?per_page=100"
    response = requests.get(url).json()
    all_branches = [branch_info["name"] for branch_info in response]
    if branches_blacklist is None:
        branches_blacklist = []
    return [b for b in all_branches if b not in branches_blacklist]


def get_pull_requests(repo: str) -> list[str]:
    url = (
        f"https://api.github.com/repos/nycplanning/{repo}/pulls?state=open&per_page=100"
    )
    response = requests.get(url).json()
    return [str(pr_info["number"]) for pr_info in response]


def get_workflow(repo: str, name: str):
    url = f"{BASE_URL}/{repo}/actions/workflows/{name}"
    r = requests.get(url, headers=headers)
    return r.json()


def _get_workflow_runs_helper(
    url: str, params: Dict[str, Any] | None = None
) -> list[dict]:
    response = requests.get(url, headers=headers, params=params).json()
    if "workflow_runs" in response:
        return response["workflow_runs"]
    else:
        raise Exception(
            f"Error retreiving workflow runs from Github. If error persists, contact Data Engineering. Github API response: {response}"
        )


def get_workflow_runs(
    repo: str,
    workflow_name: str | None = None,
    items_per_page: int = 100,
    total_items: int = 100,
    page_start: int = 0,
) -> list[WorkflowRun]:
    if items_per_page > 100:
        raise ValueError("github api does not support greater than 100 items per page")
    if workflow_name:
        url = f"{BASE_URL}/{repo}/actions/workflows/{workflow_name}/runs"
    else:
        url = f"{BASE_URL}/{repo}/actions/runs"
    workflows: List[Dict] = []
    page = page_start
    res: List[Dict] = []
    while (
        (total_items is None)
        or (total_items is not None and len(workflows) < total_items)
    ) and (page == page_start or len(res) != 0):
        page += 1
        res = _get_workflow_runs_helper(
            url, params={"per_page": items_per_page, "page": page}
        )
        workflows += res
        if total_items is not None and len(workflows) < total_items:
            workflows = workflows[:total_items]
    return [WorkflowRun.parse_from_github(workflow) for workflow in workflows]


def dispatch_workflow(
    repo: str, workflow_name: str, branch: str = "main", **inputs
) -> None:
    params = {"ref": branch, "inputs": inputs}
    url = f"{BASE_URL}/{repo}/actions/workflows/{workflow_name}/dispatches"
    response = requests.post(url, headers=headers, json=params)
    if response.status_code != 204:
        print(response.content)
        raise Exception(
            f"Dispatch workflow failed with status code {response.status_code}"
        )
