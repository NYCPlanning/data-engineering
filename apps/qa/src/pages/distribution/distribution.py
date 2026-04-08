def distribution():
    import streamlit as st

    from . import helpers

    st.subheader("Version comparison between Bytes and Open Data")
    st.markdown(
        body="""
        This table shows the latest versions of datasets on Bytes and Open Data, along with links to them.
        
        The "up to date" column indicates whether the latest version on Open Data is probably the same as the latest version on Bytes, based on a fuzzy comparison of version strings.
    """
    )
    button_get_versions = st.sidebar.button(
        label="Get versions",
        help="Clear local cache and get versions.",
        use_container_width=True,
    )
    button_get_cached_versions = st.sidebar.button(
        "Get cached versions",
        help="Get cached versions (if available) to avoid long-running operation.",
        use_container_width=True,
    )
    if button_get_versions:
        with st.spinner("Getting versions from Bytes and Open Data ..."):
            versions = helpers.get_versions()
        st.cache_data.clear()

    if button_get_cached_versions:
        with st.spinner(
            "Getting cached versions (if available) from Bytes and Open Data ..."
        ):
            versions = helpers.get_versions()
        st.dataframe(
            versions.reset_index(),
            width="stretch",
            hide_index=True,
            column_config={
                "bytes_url": st.column_config.LinkColumn(
                    "Bytes URL", display_text="URL"
                ),
                "open_data_url": st.column_config.LinkColumn(
                    "Open Data URL", display_text="URL"
                ),
            },
        )

    if not button_get_versions and not button_get_cached_versions:
        st.info("Click a button on the left to fetch and display versions.")

    st.subheader("Helpful links")
    st.markdown(
        body="""
        - Github action to distribute from Bytes to Open Data: https://github.com/NYCPlanning/data-engineering/actions/workflows/distribute_socrata_from_bytes.yml
        - Open Data page to sign in and publish revisions: https://opendata.cityofnewyork.us/
        - Product Metadata repo: https://github.com/NYCPlanning/product-metadata
    """
    )
