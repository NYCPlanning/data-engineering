def distribution():
    import streamlit as st
    from shared.components.github import dispatch_workflow_button

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
        st.cache_data.clear()
    if button_get_versions or button_get_cached_versions:
        # Buttons are only True on the run that handles the click, but clicking Distribute
        # below reruns the page — so remember that versions were asked for.
        st.session_state["show_versions"] = True

    if not st.session_state.get("show_versions"):
        st.info("Click a button on the left to fetch and display versions.")
    else:
        with st.spinner("Getting versions from Bytes and Open Data ..."):
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

        groups = helpers.outdated_groups(versions)
        st.subheader(f"Outdated on Open Data ({len(groups)})")
        st.markdown(
            f"""
            Distributing opens a revision on Open Data but does not publish it — sign in to
            [Open Data](https://opendata.cityofnewyork.us/) to review and publish it.
            Runs land in [{helpers.DISTRIBUTE_WORKFLOW}]({helpers.DISTRIBUTE_WORKFLOW_URL}).

            A dataset's version on Open Data is read back out of its description, which is
            rendered from `current_version_*` in
            [product-metadata/snippets/strings.yml]({helpers.STRINGS_YML_URL}). Distributing
            stamps whatever that file says on `main`, so a dataset can only be brought up to
            date once its `current_version_*` matches Bytes — bump it there first.
        """
        )

        last_dispatch = st.session_state.get("last_distribution_dispatch")
        if last_dispatch:
            st.success(
                f"Dispatched distribution for {last_dispatch}. "
                f"Track it in [GitHub Actions]({helpers.DISTRIBUTE_WORKFLOW_URL})."
            )

        if not groups:
            st.info("Every dataset on Open Data matches the version on Bytes.")

        def record_dispatch(name: str):
            st.session_state["last_distribution_dispatch"] = name

        for group in groups:
            group_name = f"{group.product} → {group.destination_id}"
            datasets = sorted(group.ready["dataset"])
            summary, button = st.columns((6, 1), vertical_alignment="center")
            with summary:
                st.markdown(f"**{group.product}** → `{group.destination_id}`")
                for row in group.ready.itertuples():
                    st.caption(
                        f"{row.dataset} — bytes `{row.bytes_version}`, open data `{row.open_data_versions}`"
                    )
                for row in group.blocked.itertuples():
                    st.caption(
                        f"⚠️ {row.dataset} — bytes `{row.bytes_version}`, open data "
                        f"`{row.open_data_versions}`, product-metadata `{row.metadata_version}`. "
                        "Bump `current_version` in strings.yml before distributing."
                    )
            with button:
                dispatch_workflow_button(
                    helpers.DISTRIBUTE_REPO,
                    helpers.DISTRIBUTE_WORKFLOW,
                    key=f"distribute-{group.product}-{group.destination_id}",
                    label="Distribute",
                    disabled=not datasets,
                    run_after=lambda name=group_name: record_dispatch(name),
                    product=group.product,
                    source="bytes",
                    datasets=",".join(datasets),
                    destination_ids=group.destination_id,
                    # Sent explicitly rather than relying on the workflow default, so a change
                    # to that default can't turn a click here into a public publish.
                    publish=False,
                )

    st.subheader("Helpful links")
    st.markdown(
        body="""
        - Github action to distribute from Bytes to Open Data: https://github.com/NYCPlanning/data-engineering/actions/workflows/distribute_socrata_from_bytes.yml
        - Open Data page to sign in and publish revisions: https://opendata.cityofnewyork.us/
        - Product Metadata repo: https://github.com/NYCPlanning/product-metadata
    """
    )
