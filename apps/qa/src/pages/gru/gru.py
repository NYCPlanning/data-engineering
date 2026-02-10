def gru():
    import time

    import streamlit as st

    from .components import check_table, source_table
    from .constants import qa_checks, readme_markdown_text
    from .helpers import (
        get_geosupport_versions,
        get_qaqc_runs,
        run_all_workflows,
    )

    st.markdown(
        "<style>button{text-align:left; margin:0}.stDownloadButton{max-width:195px;}</style>",
        unsafe_allow_html=True,
    )

    geosupport_version = st.sidebar.selectbox(
        label="Choose a Geosupport version",
        options=list(get_geosupport_versions().keys()),
    )

    st.header("GRU QAQC")
    st.write(
        """This page runs automated QAQC checks for various GRU-maintained files, displays source data info and makes outputs available for download.  \n
Checks are performed either by comparing files to each other or by comparing a file to the latest Geosupport release.
To perform a check, hit a button in the table below. The status column has a link to the latest Github workflow run for a given check  \n
Github repo found [here](https://github.com/NYCPlanning/db-gru-qaqc/)."""
    )

    st.header("Latest Source Data")
    source_table()

    st.header(f"QAQC Checks - Geosupport {geosupport_version}")
    workflows = get_qaqc_runs(geosupport_version)
    not_running_workflows = [
        action_name
        for action_name in qa_checks["action_name"]
        if action_name not in workflows or (not workflows[action_name].is_running)
    ]
    run_all_workflows(not_running_workflows, geosupport_version)
    check_table(workflows, geosupport_version=geosupport_version)

    st.header("README")
    st.markdown(readme_markdown_text)

    # this state gets set when an action is triggered, set to false once it's complete
    running = any([workflow.is_running for workflow in workflows.values()])
    while running:
        time.sleep(5)
        st.rerun()

    # refresh after 10 min
    while not running:
        time.sleep(600)
        st.rerun()
