import os
import requests
import streamlit as st

ORG = "NYCPlanning"
PERSONAL_TOKEN = os.environ.get("GHP_TOKEN", None)
headers = {"Authorization": "Bearer %s" % PERSONAL_TOKEN}
BASE_URL = f"https://api.github.com/repos/{ORG}"


def parse_workflow(workflow):  ## json input
    return {
        "status": workflow["status"],
        "conclusion": workflow["conclusion"],
        "timestamp": workflow["updated_at"],
        "url": workflow["html_url"],
    }


def workflow_is_running(workflow: dict):
    print(workflow)
    return workflow.get("status") in ["queued", "in_progress"]


def get_default_branch(repo: str):
    url = f"https://api.github.com/repos/nycplanning/{repo}"
    response = requests.get(url).json()
    return response["default_branch"]


def get_branches(repo: str, branches_blacklist: list):
    url = f"https://api.github.com/repos/nycplanning/{repo}/branches"
    response = requests.get(url).json()
    all_branches = [branch_info["name"] for branch_info in response]
    return [b for b in all_branches if b not in branches_blacklist]


def get_workflow(repo, name):
    url = f"{BASE_URL}/{repo}/actions/workflows/{name}"
    r = requests.get(url, headers=headers)
    return r.json()


def __get_workflow_runs_helper(url, params=None):
    response = requests.get(url, headers=headers, params=params).json()
    if "workflow_runs" in response:
        return response["workflow_runs"]
    else:
        raise Exception(
            f"Error retreiving workflow runs from Github. If error persists, contact Data Engineering. Github API response: {response}"
        )


def get_workflow_runs(
    repo, workflow_name=None, items_per_page=100, total_items=100, page_start=0
):  ## 100 is to be max
    if workflow_name:
        url = f"{BASE_URL}/{repo}/actions/workflows/{workflow_name}/runs"
    else:
        url = f"{BASE_URL}/{repo}/actions/runs"
    workflows = []
    page = 0
    res = []
    while (total_items is None and (page == 0 or len(res) != 0)) or (
        total_items is not None and len(workflows) < total_items
    ):
        page += 1
        res = __get_workflow_runs_helper(
            url, params={"per_page": items_per_page, "page": page}
        )
        print(len(res))
        workflows += res
        if total_items is not None and len(workflows) < total_items:
            workflows = workflows[:total_items]
    print(len(workflows))
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


def dispatch_workflow_button(
    repo, workflow_name, key, label="Run", disabled=False, run_after=None, **inputs
):
    def on_click():
        dispatch_workflow(repo, workflow_name, **inputs)
        if run_after is not None:
            run_after()

    return st.button(label, key=key, on_click=on_click, disabled=disabled)
