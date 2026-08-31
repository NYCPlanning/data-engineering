import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
import streamlit as st
from app_globals import ROOT_PATH

from dcpy.configuration import INGEST_DEF_DIR
from dcpy.connectors.edm import publishing
from dcpy.lifecycle.connector_registry import connectors
from dcpy.lifecycle.ingest import plan
from dcpy.lifecycle.ingest.connectors import get_processed_datastore_connector
from dcpy.lifecycle.scripts.version_compare import render_version
from dcpy.utils import s3
from dcpy.utils.git import github

from .constants import (
    CHECKS_REPO,
    CHECKS_WORKFLOW,
    SourceDataset,
    bucket,
    qa_checks,
    source_datasets,
)

# TEMPLATE_DIR isn't set in the app container and its default is relative to the working
# directory, which streamlit doesn't set to the repo root. The repo is mounted whole, so fall
# back to a path off the app's own location.
INGEST_TEMPLATE_DIR = (
    Path(INGEST_DEF_DIR)
    if Path(INGEST_DEF_DIR).is_dir()
    else ROOT_PATH / "ingest_templates"
)

UP_TO_DATE = "up to date"
BEHIND = "behind"
UNKNOWN = "unknown"
READ_IN_PLACE = "read in place"


def public_url(key: str) -> str:
    """Unsigned, non-expiring link. QAQC outputs are uploaded public-read."""
    endpoint = os.environ["AWS_S3_ENDPOINT"].rstrip("/")
    return f"{endpoint}/{bucket}/{quote(key)}"


def _read_in_place(source: SourceDataset) -> tuple[str, datetime | None]:
    """The version of a GIS upload the checks would read out of edm-publishing.

    Nothing archives these to edm-recipes, so there is no config to ask for a version. The
    upload timestamp is the closest thing to an archival date.
    """
    prefix = f"gru/{source.id}/"
    folders = s3.get_subfolders(bucket, prefix)
    if "latest" in folders:
        folders.remove("latest")
    version = max(folders)
    timestamp = s3.get_metadata(
        bucket, f"{prefix}{version}/{source.id}.zip"
    ).last_modified
    return version, timestamp


def _archived(source: SourceDataset) -> tuple[str, datetime | None]:
    if source.upstream_kind is None:
        return _read_in_place(source)
    config = get_processed_datastore_connector().get_sparse_config(
        key=source.id, version="latest"
    )
    return config.version, config.run_timestamp


def _upstream(source: SourceDataset) -> str | None:
    """The newest version at the origin, or None where the connector can't report one.

    Bytes datasets need an explicit key: their templates fetch a raw versioned URL rather than
    going through the bytes connector, so asking the template's own source yields nothing.
    """
    match source.upstream_kind:
        case "template":
            definition = plan.read_definition_file(
                INGEST_TEMPLATE_DIR / f"{source.id}.yml"
            )
            return plan.get_version(definition.source)
        case "bytes":
            return connectors["bytes"].get_latest_version(source.upstream_key)
        case "publishing":
            return publishing.get_latest_version(source.upstream_key)
        case _:
            return None


def classify(
    source: SourceDataset,
    archived_version: str | Exception | None,
    upstream_version: str | Exception | None,
) -> str:
    """How the archived version stands against what the origin offers.

    A version we failed to fetch is `unknown`, never `behind`: an unreachable origin says
    nothing about whether the archive is current, and calling it stale would send someone to
    re-ingest data that is already fine.
    """
    if source.upstream_kind is None:
        return READ_IN_PLACE
    if isinstance(archived_version, Exception) or isinstance(
        upstream_version, Exception
    ):
        return UNKNOWN
    if not archived_version or not upstream_version:
        return UNKNOWN
    # Both strings come from the same origin in the same format, so they compare directly. This
    # is not the Bytes-against-Open-Data case that version_compare has to fuzzy-match.
    return UP_TO_DATE if archived_version == upstream_version else BEHIND


@st.cache_data(ttl=600, show_spinner="Checking source data versions ...")
def get_source_status() -> pd.DataFrame:
    """What is archived for each source, against what the origin currently offers.

    Cached for the same 10 minutes the page waits before rerunning itself, so an idle page
    refetches once a cycle rather than on every rerun.
    """
    rows = []
    for source in source_datasets.values():
        archived_version: str | Exception | None = None
        archived_on: datetime | None = None
        upstream_version: str | Exception | None = None

        try:
            archived_version, archived_on = _archived(source)
        except Exception as error:
            archived_version = error
        try:
            upstream_version = _upstream(source)
        except Exception as error:
            upstream_version = error

        rows.append(
            {
                "dataset": source.id,
                "archived_version": render_version(archived_version),
                "archived_on": archived_on.date() if archived_on else None,
                "latest_version": render_version(upstream_version),
                "status": classify(source, archived_version, upstream_version),
                "refreshed_by": source.refresh,
            }
        )
    return pd.DataFrame(rows)


def behind_sources(status: pd.DataFrame) -> pd.DataFrame:
    """Rows an ingest run would actually fix.

    Deliberately excludes `unknown`: a version we failed to fetch is not evidence of a stale
    archive, and dispatching without a version we trust would archive under the wrong one.
    """
    return status[status["status"] == BEHIND]


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
            CHECKS_REPO,
            CHECKS_WORKFLOW,
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
                CHECKS_REPO,
                CHECKS_WORKFLOW,
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
        if re.match(r"^\d+\.\d+\.\d+$", image["name"]):
            major, minor, _ = image["name"].split(".")
            code = f"{major}{chr(int(minor) + 96)}"
            if code not in images_by_code:
                images_by_code[code] = image["name"]
    return images_by_code
