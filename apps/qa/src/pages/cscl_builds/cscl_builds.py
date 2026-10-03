BUILD_REPO = "data-engineering"
BUILD_WORKFLOW = "build.yml"
BUILD_WORKFLOW_URL = (
    f"https://github.com/NYCPlanning/{BUILD_REPO}/actions/workflows/{BUILD_WORKFLOW}"
)


def cscl_builds() -> None:
    import streamlit as st

    from dcpy.configuration import PUBLISHING_BUCKET
    from dcpy.utils.git import github
    from shared.components.github import (
        find_run,
        list_dispatchable_branches,
        status_details,
    )

    from . import helpers

    @st.dialog("Run build")
    def _run_build_dialog(version: str) -> None:
        st.caption(f"`{BUILD_WORKFLOW}` for `cscl` version `{version}`")
        build_note = st.text_input("Build note", key=f"build_note_{version}")
        build_name = helpers.build_name_for(version, build_note)
        st.caption(f"Build name: `{build_name}`")
        branches = list_dispatchable_branches(BUILD_REPO)
        branch = st.selectbox(
            "Branch to dispatch against",
            options=branches,
            index=branches.index("main") if "main" in branches else 0,
            key=f"build_branch_{version}",
        )
        if st.button("Dispatch", key=f"build_confirm_{version}"):
            try:
                github.dispatch_workflow(
                    BUILD_REPO,
                    BUILD_WORKFLOW,
                    branch=branch,
                    dataset_name="cscl",
                    recipe_file="recipe",
                    version=version,
                    build_name=build_name,
                    build_note=build_note,
                    # Sent explicitly rather than relying on the workflow defaults, so a
                    # change to those defaults can't silently change what a click here does.
                    test_severity="error",
                    dev_image=False,
                    logging_level="INFO",
                )
            except Exception as e:
                st.error(str(e))
                return
            st.session_state[f"cscl_build_dispatched:{version}"] = True
            st.rerun()

    st.title("CSCL Builds")
    st.markdown(
        """
        Each row is a version of the CSCL fgdb that's been ingested to `edm-recipes`, with any
        draft versions already promoted for it. Kicking off a build runs
        [`build.yml`](%s) for `cscl` at that ingested version; a successful build is promoted
        to draft automatically by the workflow itself.
    """
        % BUILD_WORKFLOW_URL
    )

    assert PUBLISHING_BUCKET, "PUBLISHING_BUCKET must be set"

    ingested_versions = helpers.get_ingested_versions()
    if not ingested_versions:
        st.info(f"No ingested versions found for `{helpers.RAW_DATASET_ID}` yet.")
        return

    for version in ingested_versions:
        st.divider()
        # Links to the uploaded source file, not edm-recipes: the ingested data for one
        # version is spread across ~50 separate per-layer datasets there, so there's no
        # single folder to point at. The inbox upload is the one place it's all in one spot.
        inbox_url = helpers.directory_url(
            helpers.INBOX_BUCKET, f"qa_app/inbox/{helpers.RAW_DATASET_ID}/{version}/"
        )
        st.markdown(f"### [{version}]({inbox_url})")

        revisions = helpers.get_draft_revisions(version)
        if revisions:
            links = [
                f"[{revision}]({helpers.directory_url(PUBLISHING_BUCKET, f'{helpers.PRODUCT}/draft/{version}/{revision}/')})"
                for revision in revisions
            ]
            st.markdown("Drafts: " + ", ".join(links))
        else:
            st.caption("No drafts yet.")

        run_name = f"🏗️ Build a Dataset: cscl {version}"
        run = find_run(BUILD_REPO, BUILD_WORKFLOW, run_name)
        running = run is not None and run.is_running

        dispatch_key = f"cscl_build_dispatched:{version}"

        if st.button("Run build", key=f"build-{version}", disabled=running):
            _run_build_dialog(version)

        if running or st.session_state.get(dispatch_key):
            st.info(f"Dispatched. Track it in [GitHub Actions]({BUILD_WORKFLOW_URL}).")
            if run:
                status_details(run)
