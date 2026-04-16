def cscl():
    import streamlit as st

    from . import helpers

    st.title("CSCL QA")
    st.markdown(
        body="""
        Review differences between the development and production CSCL ETL files.
        Select a build version from the sidebar to load the diffs summary.
    """
    )

    builds = helpers.get_builds()
    if builds.index("nightly_qa") is not None:
        # If the "nightly_qa" build is present, select it by default
        default_index = builds.index("nightly_qa")
    else:
        default_index = None
    selected_build = st.sidebar.selectbox(
        "Select a build",
        options=builds,
        index=default_index,
        placeholder="Choose a build version...",
    )

    if not selected_build:
        st.info("Select a build version from the sidebar to get started.")
        return

    with st.spinner(f"Loading data for build {selected_build} ..."):
        prod_version = helpers.get_build_version(selected_build)
        diffs_summary = helpers.get_diffs_summary(selected_build)

    st.info(f"Prod version used to generate diffs: **{prod_version}**")

    st.subheader("Diffs Summary")
    st.markdown(
        body="""
        This table shows which files have differences between the current and previous build.
        
        The **Has diffs** column indicates whether any rows changed. The **# of rows with diffs** column
        shows the count of changed rows.
    """
    )

    st.dataframe(
        diffs_summary,
        width="stretch",
        hide_index=True,
        column_config={
            "Has diffs": st.column_config.CheckboxColumn("Has diffs"),
            "# of rows with diffs": st.column_config.NumberColumn(
                "# of rows with diffs"
            ),
        },
    )
