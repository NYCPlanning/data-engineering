from datetime import datetime
import time
import streamlit as st
import requests
import re

from dcpy.utils import s3
from dcpy.utils.git import github
from dcpy.connectors.edm import recipes
from .constants import qa_checks


def get_source_version(dataset: str) -> dict:
    if dataset == "dcp_saf":
        bucket = "edm-publishing"
        prefix = "gru/dcp_saf/"
        folders = s3.get_subfolders(bucket, prefix)
        if "latest" in folders:
            folders.remove("latest")
        version = max(folders)
        timestamp = s3.get_metadata(
            bucket, f"{prefix}{version}/dcp_saf.zip"
        ).last_modified
    else:
        config_obj = recipes.get_config_obj(dataset)
        if "dataset" in config_obj:
            version = config_obj["dataset"]["version"]
            timestamp_str = config_obj["execution_details"]["timestamp"]
        else:
            version = config_obj["version"]
            timestamp_str = config_obj["run_details"]["timestamp"]
        timestamp = datetime.fromisoformat(timestamp_str)
    return {"name": dataset, "version": version, "timestamp": timestamp}


@st.cache_data(ttl=120)
def get_source_versions() -> dict[str, dict]:
    versions = {}
    for dataset in [source for sources in qa_checks["sources"] for source in sources]:
        versions[dataset] = get_source_version(dataset)
    return versions


def map_geosupport_version(patched_version: str) -> str:
    major, minor, _ = patched_version.split(".")
    return f"{major}{chr(int(minor) + 96)}"  ## converts 1 to 'a', 2 to 'b', etc


def get_qaqc_runs(geosupport_version: str) -> dict[str, github.WorkflowRun]:
    qa_check_names = qa_checks["action_name"].values
    workflow_runs: dict[str, github.WorkflowRun] = {}
    raw_workflow_runs: list[github.WorkflowRun] = []
    page = 0
    while len(workflow_runs) != len(qa_checks) and (
        page == 0 or (len(raw_workflow_runs) > 0)
    ):
        raw_workflow_runs = github.get_workflow_runs(
            "db-gru-qaqc",
            "main.yml",
            page_start=page,  ## specifies manually so we can exit sooner if requirements met
        )
        if len(raw_workflow_runs) == 0:
            break
        for run in raw_workflow_runs:
            match = re.match(r"^(\d+\.\d+\.\d+)\_(.+)$", run.name)
            if match:
                gs_version, name = (
                    map_geosupport_version(match.group(1)),
                    match.group(2),
                )
                if (
                    name in qa_check_names
                    and (gs_version == geosupport_version)
                    and (name not in workflow_runs)
                ):
                    workflow_runs[name] = run
        page += 1
    return workflow_runs


def run_all_workflows(actions: list[str], geosupport_version: str) -> bool:
    def on_click():
        for action in actions:
            github.dispatch_workflow(
                "db-gru-qaqc",
                "main.yml",
                name=action,
                geosupport_version=get_geosupport_versions()[geosupport_version],
            )
        time.sleep(2)

    return st.button("Run all", key="all", on_click=on_click)


@st.cache_data(ttl=600)
def get_geosupport_versions() -> dict[str, str]:
    images = requests.get(
        "https://hub.docker.com/v2/repositories/nycplanning/docker-geosupport/tags?page_size=1000"
    ).json()["results"]
    images_by_code = {}
    for image in images:
        if re.match("^\d+\.\d+\.\d+$", image["name"]):
            major, minor, _ = image["name"].split(".")
            code = f"{major}{chr(int(minor) + 96)}"
            if code not in images_by_code:
                images_by_code[code] = image["name"]
    return images_by_code
