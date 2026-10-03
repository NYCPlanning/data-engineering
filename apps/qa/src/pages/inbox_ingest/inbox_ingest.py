INGEST_REPO = "data-engineering"
INGEST_WORKFLOW = "ingest_single.yml"
INGEST_WORKFLOW_URL = (
    f"https://github.com/NYCPlanning/{INGEST_REPO}/actions/workflows/{INGEST_WORKFLOW}"
)

SELECTED_DATASET_KEY = "inbox_ingest_selected_dataset"


def inbox_ingest() -> None:
    import streamlit as st

    from . import helpers

    st.title("Inbox Ingest")

    dataset_id = st.session_state.get(SELECTED_DATASET_KEY)
    if dataset_id:
        _dataset_view(st, helpers, dataset_id)
    else:
        _dataset_index(st, helpers)


def _dataset_index(st, helpers) -> None:
    st.markdown(
        """
        Each of these ingest templates reads its source from
        `edm-private/qa_app/inbox/` - a template opts in just by pointing its source key
        there. Pick one to see its uploaded versions and ingest status.
    """
    )

    datasets = helpers.list_inbox_datasets()
    if not datasets:
        st.info(
            "No ingest templates currently point at `edm-private/qa_app/inbox/`. Point a "
            "template's `source.key` there to make it available here."
        )
        return

    for dataset_id in datasets:
        name_col, button_col = st.columns((5, 1), vertical_alignment="center")
        with name_col:
            st.markdown(f"**{dataset_id}**")
        with button_col:
            if st.button("View", key=f"select-{dataset_id}"):
                st.session_state[SELECTED_DATASET_KEY] = dataset_id
                st.rerun()
        st.divider()


def _dataset_view(st, helpers, dataset_id: str) -> None:
    from dcpy.utils.git import github
    from shared.components.github import (
        find_run,
        list_dispatchable_branches,
        status_details,
    )

    @st.dialog("Run ingest")
    def _run_ingest_dialog(version: str) -> None:
        st.caption(f"`{INGEST_WORKFLOW}` for `{dataset_id}` version `{version}`")
        branches = list_dispatchable_branches(INGEST_REPO)
        branch = st.selectbox(
            "Branch to dispatch against",
            options=branches,
            index=branches.index("main") if "main" in branches else 0,
            key=f"ingest_branch_{dataset_id}_{version}",
        )
        latest = st.checkbox(
            "Tag as latest",
            value=False,
            key=f"ingest_latest_{dataset_id}_{version}",
        )
        if st.button("Dispatch", key=f"ingest_confirm_{dataset_id}_{version}"):
            try:
                github.dispatch_workflow(
                    INGEST_REPO,
                    INGEST_WORKFLOW,
                    branch=branch,
                    dataset=dataset_id,
                    version=version,
                    # Sent explicitly rather than relying on the workflow defaults, so a
                    # change to those defaults can't silently change what a click here does.
                    latest=latest,
                    overwrite=False,
                )
            except Exception as e:
                st.error(str(e))
                return
            st.session_state[f"inbox_ingest_dispatched:{dataset_id}:{version}"] = True
            st.rerun()

    if st.button("← All datasets"):
        del st.session_state[SELECTED_DATASET_KEY]
        st.rerun()

    st.subheader(dataset_id)

    with st.expander("Upload a new version"):
        _upload_form(st, helpers, dataset_id)

    versions = helpers.list_inbox_versions(dataset_id)
    if not versions:
        st.info("No versions uploaded to the inbox yet.")
        return

    for version in versions:
        st.divider()
        ingested = helpers.is_fully_ingested(dataset_id, version)
        run = None
        if not ingested:
            run_name = f"Ingest Single Dataset: {dataset_id} {version}"
            run = find_run(INGEST_REPO, INGEST_WORKFLOW, run_name)
        running = run is not None and run.is_running
        dispatch_key = f"inbox_ingest_dispatched:{dataset_id}:{version}"

        version_col, status_col, button_col = st.columns(
            (2, 3, 2), vertical_alignment="center"
        )
        with version_col:
            st.markdown(f"**{version}**")
        with status_col:
            if ingested:
                st.success("Ingested")
            elif run:
                status_details(run)
            elif st.session_state.get(dispatch_key):
                st.info(
                    f"Dispatched. Track it in [GitHub Actions]({INGEST_WORKFLOW_URL})."
                )
            else:
                st.caption("Not yet ingested.")
        with button_col:
            if not ingested and st.button(
                "Run ingest", key=f"ingest-{dataset_id}-{version}", disabled=running
            ):
                _run_ingest_dialog(version)


def _upload_form(st, helpers, dataset_id: str) -> None:
    version = st.text_input("Version", key=f"upload_version_{dataset_id}")
    if not version:
        return

    try:
        resolved = helpers.resolve_destination(dataset_id, version)
    except Exception as e:
        st.error(f"Could not resolve an upload destination: {e}")
        return

    filename = helpers.expected_filename(resolved.key)
    st.caption(f"Uploads to `s3://{resolved.bucket}/{resolved.key}`")

    # Checked once per (dataset, version), not on every rerun: our own upload below makes
    # this exact check start returning True, which would otherwise re-block the form on the
    # very rerun that follows a successful upload - this flag lets a confirmation display
    # instead of the form re-reporting its own just-completed upload as a conflict.
    upload_state_key = f"inbox_ingest_uploaded:{dataset_id}:{version}"
    if st.session_state.get(upload_state_key):
        st.success(f"Uploaded `{version}` - see it in the table below.")
        return

    if helpers.is_in_inbox(resolved) or helpers.is_fully_ingested(dataset_id, version):
        st.error(
            f"Version `{version}` already exists - either already uploaded to the inbox, "
            "or already fully ingested. Pick a new version to upload again."
        )
        return

    uploaded_file = st.file_uploader(
        f"Upload `{filename}`", key=f"uploader_{dataset_id}"
    )
    if uploaded_file is not None and uploaded_file.name != filename:
        st.error(f"Expected a file named `{filename}`, got `{uploaded_file.name}`.")
        return

    if uploaded_file is not None and st.button(
        "Upload", key=f"upload_btn_{dataset_id}"
    ):
        with st.spinner(f"Uploading to s3://{resolved.bucket}/{resolved.key} ..."):
            helpers.upload(resolved.bucket, resolved.key, uploaded_file)
        st.session_state[upload_state_key] = True
        st.rerun()
