import pytz
import pandas as pd
from urllib.error import HTTPError
import streamlit as st
import time

from dcpy.utils.git import github
from dcpy.utils import s3
from src.shared.components.github import dispatch_workflow_button
from .constants import qa_checks, bucket
from .helpers import get_source_versions, get_geosupport_versions


def status_details(workflow_run: github.WorkflowRun) -> None:
    timestamp = workflow_run.timestamp.astimezone(pytz.timezone("US/Eastern")).strftime(
        "%Y-%m-%d %H:%M"
    )

    def format(status: str) -> str:
        return f"{status}  \n[{timestamp}]({workflow_run.url})"

    if workflow_run.is_running:
        st.warning(format(workflow_run.status.capitalize().replace("_", " ")))
        st.spinner()
    elif workflow_run.status == "completed":
        if workflow_run.conclusion == "success":
            st.success(format("Success"))
        elif workflow_run.conclusion == "cancelled":
            st.info(format("Cancelled"))
        elif workflow_run.conclusion == "failure":
            st.error(format("Failed"))
        else:
            st.write(workflow_run.conclusion)


def source_table() -> None:
    column_widths = (4, 5, 3)
    cols = st.columns(column_widths)
    fields = ["Name", "Latest version archived by DE", "Date of archival"]
    for col, field_name in zip(cols, fields):
        col.write(f"**{field_name}**")
    source_versions = get_source_versions()
    for source in source_versions:
        col1, col2, col3 = st.columns(column_widths)
        col1.write(source)
        col2.write(source_versions[source]["version"])
        col3.write(source_versions[source]["timestamp"].strftime("%Y-%m-%d"))


def check_table(
    workflows: dict[str, github.WorkflowRun], geosupport_version: str
) -> None:
    column_widths = (3, 3, 4, 3, 2)
    cols = st.columns(column_widths)
    fields = ["Name", "Sources", "Latest results", "Status"]
    for i, field in enumerate(fields):
        cols[i].write(f"**{field}**")

    for _, check in qa_checks.iterrows():
        action_name = check["action_name"]
        if action_name in workflows:
            name, sources, outputs, status, run = st.columns(column_widths)
            workflow_run = workflows[action_name]
            running = workflow_run.is_running

            name.write(check["display_name"])

            s3_folder = f"db-gru-qaqc/{geosupport_version}/{action_name}/latest"

            if not running:
                with sources:
                    try:
                        path = s3.get_presigned_get_url(
                            bucket, f"{s3_folder}/versions.csv", 5
                        )
                        versions = pd.read_csv(path)
                        st.download_button(
                            label="\n".join(check["sources"]),
                            data=versions.to_csv(index=False).encode("utf-8"),
                            file_name="versions.csv",
                            mime="text/csv",
                            help=versions.to_markdown(index=False),
                        )
                    # TODO - this should probably use publishing api, FileNotFoundError
                    # However, GRU is a special "product" that doesn't follow our norms yet
                    except HTTPError as e:
                        print(e)
                        st.error("Not found")

                filenames = sorted(s3.get_filenames(bucket, s3_folder))

                def get_url(f: str) -> str:
                    """
                    page refreshes every 10 min as set in gru.py
                    urls valid just past that
                    """
                    return s3.get_presigned_get_url(bucket, f"{s3_folder}/{f}", 610)

                files = "  \n".join(
                    [
                        f"[{filename}]({get_url(filename)})"
                        for filename in filenames
                        if filename != "versions.csv"
                    ]
                )
                outputs.write(files)

            with status:
                status_details(workflow_run)

        else:
            name, column, run = st.columns((3, 10, 2))
            name.write(check["display_name"])
            with column:
                st.info(
                    format(
                        f"Check has not been run yet for Geosupport {geosupport_version}"
                    )
                )
            running = False

        with run:
            dispatch_workflow_button(
                "db-gru-qaqc",
                "main.yml",
                disabled=running,
                key=check["action_name"],
                name=check["action_name"],
                geosupport_version=get_geosupport_versions()[geosupport_version],
                run_after=lambda: time.sleep(2),
            )  ## refresh after 2 so that status has hopefully
