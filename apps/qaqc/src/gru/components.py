from datetime import datetime
import pytz
import pandas as pd
import streamlit as st
import time

from dcpy.connectors.github import workflow_is_running
from src.github import dispatch_workflow_button
from src.gru.constants import tests
from src.gru.helpers import get_source_versions, get_geosupport_versions


def status_details(workflow):
    timestamp = (
        workflow["timestamp"]
        .astimezone(pytz.timezone("US/Eastern"))
        .strftime("%Y-%m-%d %H:%M")
    )
    format = lambda status: f"{status}  \n[{timestamp}]({workflow['url']})"
    if workflow["status"] in ["queued", "in_progress"]:
        st.warning(format(workflow["status"].capitalize().replace("_", " ")))
        st.spinner()
    elif workflow["status"] == "completed":
        if workflow["conclusion"] == "success":
            st.success(format("Success"))
        elif workflow["conclusion"] == "cancelled":
            st.info(format("Cancelled"))
        elif workflow["conclusion"] == "failure":
            st.error(format("Failed"))
        else:
            st.write(workflow["conclusion"])


def source_table():
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
        col3.write(source_versions[source]["date"])


def check_table(workflows, geosupport_version):
    column_widths = (3, 3, 4, 3, 2)
    cols = st.columns(column_widths)
    fields = ["Name", "Sources", "Latest results", "Status", "Run Check"]
    for col, field_name in zip(cols, fields):
        col.write(f"**{field_name}**")

    for _, test in tests.iterrows():
        action_name = test["action_name"]

        name, sources, outputs, status, run = st.columns(column_widths)

        name.write(test["display_name"])

        folder = f"https://edm-publishing.nyc3.digitaloceanspaces.com/db-gru-qaqc/{geosupport_version}/{action_name}/latest"

        with sources:
            try:
                versions = pd.read_csv(f"{folder}/versions.csv")
                st.download_button(
                    label="\n".join(test["sources"]),
                    data=versions.to_csv(index=False).encode("utf-8"),
                    file_name="versions.csv",
                    mime="text/csv",
                    help=versions.to_markdown(index=False),
                )
            except:
                pass

        files = "  \n".join(
            [f"[{filename}]({folder}/{filename}.csv)" for filename in test["files"]]
        )
        outputs.write(files)

        running = workflow_is_running(workflows.get(action_name, {}))

        with status:
            if action_name in workflows:
                workflow = workflows[action_name]
                status_details(workflow)
            else:
                st.info(format("No past run found"))

        with run:
            dispatch_workflow_button(
                "db-gru-qaqc",
                "main.yml",
                disabled=running,
                key=test["action_name"],
                name=test["action_name"],
                geosupport_version=get_geosupport_versions()[geosupport_version],
                run_after=lambda: time.sleep(2),
            )  ## refresh after 2 so that status has hopefully
