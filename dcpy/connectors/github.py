import os
import requests
from dateutil.parser import parse
from typing import Optional

ORG = "NYCPlanning"
PERSONAL_TOKEN = os.environ["GHP_TOKEN"]
headers = {"Authorization": "Bearer %s" % PERSONAL_TOKEN}
BASE_URL = f"https://api.github.com/repos/{ORG}"


def parse_workflow(workflow):  ## json input
    return {
        "status": workflow["status"],
        "conclusion": workflow["conclusion"],
        "timestamp": parse(workflow["updated_at"], fuzzy=True),
        "url": workflow["html_url"],
    }


def workflow_is_running(workflow: dict):
    return workflow.get("status") in ["queued", "in_progress"]


def get_default_branch(repo: str):
    url = f"https://api.github.com/repos/nycplanning/{repo}"
    response = requests.get(url).json()
    return response["default_branch"]


def get_branches(repo: str, branches_blacklist: Optional[list] = None):
    url = f"https://api.github.com/repos/nycplanning/{repo}/branches"
    response = requests.get(url).json()
    all_branches = [branch_info["name"] for branch_info in response]
    if branches_blacklist is None:
        branches_blacklist = []
    return [b for b in all_branches if b not in branches_blacklist]


def get_workflow(repo, name):
    url = f"{BASE_URL}/{repo}/actions/workflows/{name}"
    r = requests.get(url, headers=headers)
    return r.json()


def _get_workflow_runs_helper(url, params=None):
    response = requests.get(url, headers=headers, params=params).json()
    if "workflow_runs" in response:
        return response["workflow_runs"]
    else:
        raise Exception(
            f"Error retreiving workflow runs from Github. If error persists, contact Data Engineering. Github API response: {response}"
        )


def get_workflow_runs(
    repo, workflow_name=None, items_per_page=100, total_items=100, page_start=0
):
    if items_per_page > 100:
        raise ValueError("github api does not support greater than 100 items per page")
    if workflow_name:
        url = f"{BASE_URL}/{repo}/actions/workflows/{workflow_name}/runs"
    else:
        url = f"{BASE_URL}/{repo}/actions/runs"
    workflows = []
    page = page_start
    res = []
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
    return workflows


def dispatch_workflow(repo, workflow_name, branch="main", **inputs):
    params = {"ref": branch, "inputs": inputs}
    url = f"{BASE_URL}/{repo}/actions/workflows/{workflow_name}/dispatches"
    response = requests.post(url, headers=headers, json=params)
    if response.status_code != 204:
        print(response.content)
        raise Exception(
            f"Dispatch workflow failed with status code {response.status_code}"
        )
