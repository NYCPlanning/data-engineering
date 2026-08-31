import time
from urllib.error import HTTPError

import pandas as pd
import pytz
import streamlit as st
from shared.components.github import dispatch_workflow_button

from dcpy.utils import s3
from dcpy.utils.git import github

from .constants import (
    CHECKS_REPO,
    CHECKS_WORKFLOW,
    INGEST_REPO,
    INGEST_WORKFLOW,
    INGEST_WORKFLOW_URL,
    bucket,
    qa_checks,
)
from .helpers import behind_sources, get_geosupport_versions, get_source_status

SOURCE_COLUMNS = {
    "dataset": "Source",
    "archived_version": "Archived",
    "archived_on": "Archived on",
    "latest_version": "Latest at origin",
    "status": "Status",
    "refreshed_by": "How it refreshes",
}


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
    status = get_source_status()
    st.dataframe(
        status,
        width="stretch",
        hide_index=True,
        column_config=SOURCE_COLUMNS,
    )

    last_dispatch = st.session_state.get("last_ingest_dispatch")
    if last_dispatch:
        st.success(
            f"Dispatched ingest for {last_dispatch}. Track it in "
            f"[GitHub Actions]({INGEST_WORKFLOW_URL}). This table refreshes once the run "
            "archives the new version."
        )

    behind = behind_sources(status)
    if behind.empty:
        st.info(
            "Every source is archived at the newest version its origin offers, so the checks "
            "below will run against current data."
        )
        return

    st.warning(
        f"**{len(behind)} of these are behind.** A check run now compares against the archived "
        "version, not the one at the origin. Ingest them first."
    )

    def record_dispatch(dataset: str) -> None:
        st.session_state["last_ingest_dispatch"] = dataset

    for row in behind.itertuples():
        summary, button = st.columns((6, 1), vertical_alignment="center")
        with summary:
            st.markdown(
                f"**{row.dataset}**: archived `{row.archived_version}`, "
                f"origin has `{row.latest_version}`"
            )
            st.caption(row.refreshed_by)
        with button:
            dispatch_workflow_button(
                INGEST_REPO,
                INGEST_WORKFLOW,
                key=f"ingest-{row.dataset}",
                label="Ingest",
                run_after=lambda dataset=row.dataset: record_dispatch(dataset),
                dataset=row.dataset,
                # Pinned to the version this page actually read, so the run archives what the
                # table claims. Bytes and DevDB templates need it: their source is a raw
                # versioned URL that resolves to nothing without one.
                version=row.latest_version,
                latest=True,
                # Sent explicitly rather than relying on the workflow default, so a change to
                # that default can't turn a click here into a dev-image run.
                dev_image=False,
            )


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
                    f"Check has not been run yet for Geosupport {geosupport_version}"
                )
            running = False

        with run:
            dispatch_workflow_button(
                CHECKS_REPO,
                CHECKS_WORKFLOW,
                disabled=running,
                key=check["action_name"],
                name=check["action_name"],
                geosupport_version=get_geosupport_versions()[geosupport_version],
                run_after=lambda: time.sleep(2),
            )  ## refresh after 2 so that status has hopefully changed
