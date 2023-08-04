import time
import os
import boto3
from datetime import datetime
import streamlit as st
import requests
import re

from dcpy.git import github
from dcpy.connectors.edm import recipes
from .constants import tests
from src.digital_ocean_utils import get_datatset_config


def get_source_version(dataset):
    if dataset == "dcp_saf":
        bucket = "edm-publishing"
        prefix = "gru/dcp_saf/"
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=os.getenv("AWS_S3_ENDPOINT"),
        )
        folders = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")[
            "CommonPrefixes"
        ]
        folders = [
            folder["Prefix"].split("/")[-2]
            for folder in folders
            if "latest" not in folder["Prefix"]
        ]
        latest_version = max(folders)
        timestamp = s3.list_objects(
            Bucket=bucket, Prefix=f"{prefix}{latest_version}/dcp_saf.zip"
        )["Contents"][0]["LastModified"]
        return {
            "version": latest_version.lower(),
            "date": timestamp.strftime("%Y-%m-%d"),
        }
    else:
        config = get_datatset_config(dataset, "latest")
        if "execution_details" in config:
            timestamp = datetime.strptime(
                config["execution_details"]["timestamp"], "%Y-%m-%d %H:%M:%S"
            ).strftime("%Y-%m-%d")
        else:
            timestamp = ""
        return {"version": config["dataset"]["version"], "date": timestamp}


@st.cache_data(ttl=120)
def get_source_versions():
    versions = {}
    for dataset in [source for sources in tests["sources"] for source in sources]:
        versions[dataset] = get_source_version(dataset)
    return versions


def map_geosupport_version(patched_version):
    major, minor, _ = patched_version.split(".")
    return f"{major}{chr(int(minor)+96)}"  ## converts 1 to 'a', 2 to 'b', etc


def get_qaqc_runs(geosupport_version):
    workflows = {}
    raw_workflow_runs = []
    page = 0
    while len(workflows) != 7:
        raw_workflow_runs = github.get_workflow_runs(
            "db-gru-qaqc",
            "main.yml",
            items_per_page=30,
            total_items=30,
            page_start=page,
        )
        if len(raw_workflow_runs) == 0:
            break
        for run in raw_workflow_runs:
            match = re.match("^(\d+\.\d+\.\d+)\_(.+)$", run["name"])
            if match:
                gs_version, name = map_geosupport_version(match.group(1)), match.group(
                    2
                )
                if (
                    name in tests["action_name"].values
                    and (gs_version == geosupport_version)
                    and (name not in workflows)
                ):
                    workflows[name] = github.parse_workflow(run)
        page += 1
    return workflows


def run_all_workflows(actions, geosupport_version):
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
def get_geosupport_versions():
    images = requests.get(
        "https://hub.docker.com/v2/repositories/nycplanning/docker-geosupport/tags?page_size=1000"
    ).json()["results"]
    images_by_code = {}
    for image in images:
        if re.match("^\d+\.\d+\.\d+$", image["name"]):
            major, minor, _ = image["name"].split(".")
            code = f"{major}{chr(int(minor)+96)}"
            if code not in images_by_code:
                images_by_code[code] = image["name"]
    return images_by_code
