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
        with st.spinner("Reading product metadata ..."):
            metadata_snapshot = helpers.get_metadata_snapshot()
        with st.spinner("Getting versions from Open Data ..."):
            open_data_versions = helpers.get_open_data_versions(metadata_snapshot.keys)
        with st.spinner("Getting versions from Bytes ..."):
            bytes_versions = helpers.get_bytes_versions(metadata_snapshot.keys)
        versions = helpers.compare(
            metadata_snapshot, bytes_versions, open_data_versions
        )
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

        attention = helpers.needs_attention(versions)

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

        def record_dispatch(name: str):
            st.session_state["last_distribution_dispatch"] = name

        def render_dispatchable(groups, key_prefix):
            for group in groups:
                group_name = f"{group.product} → {group.destination_id}"
                datasets = sorted(group.ready["dataset"])
                summary, button = st.columns((6, 1), vertical_alignment="center")
                with summary:
                    st.markdown(f"**{group.product}** → `{group.destination_id}`")
                    for row in group.ready.itertuples():
                        st.caption(
                            f"{row.dataset} — bytes `{row.bytes_version}`, "
                            f"open data `{row.open_data_versions}`"
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
                        key=f"{key_prefix}-{group.product}-{group.destination_id}",
                        label="Distribute",
                        disabled=not datasets,
                        run_after=lambda name=group_name: record_dispatch(name),
                        product=group.product,
                        source="bytes",
                        datasets=",".join(datasets),
                        destination_ids=group.destination_id,
                        # Sent explicitly rather than relying on the workflow default, so a
                        # change to that default can't turn a click here into a public publish.
                        publish=False,
                    )

        at_risk = attention.would_lose_version
        if len(at_risk):
            st.warning(
                f"**{len(at_risk)} datasets would lose their version if distributed.** Their Open "
                "Data page shows a version, but product-metadata's description no longer carries "
                "a `Current version:` line — so distributing patches the weaker description over "
                "the live one. Most read as up to date below, which is exactly why this is easy "
                "to miss. Fix the description in product-metadata before distributing these."
            )
            with st.expander(f"Show the {len(at_risk)} at risk"):
                st.dataframe(
                    at_risk[
                        [
                            "product",
                            "dataset",
                            "destination_id",
                            "open_data_versions",
                            "up_to_date",
                        ]
                    ],
                    width="stretch",
                    hide_index=True,
                )

        st.subheader(f"Outdated on Open Data ({len(attention.outdated)})")
        if not attention.outdated:
            st.info(
                "Every dataset with a readable version matches the version on Bytes."
            )
        render_dispatchable(attention.outdated, "distribute")

        st.subheader(
            f"No version published to Open Data yet ({len(attention.unconfirmed)})"
        )
        st.caption(
            "No version could be read off the Open Data page, but the description does carry a "
            "`Current version:` line — so it has never been stamped, or the fetch failed. These "
            "may already be up to date; distributing publishes the version and makes it readable "
            "from then on."
        )
        render_dispatchable(attention.unconfirmed, "distribute-unconfirmed")

        no_tag = attention.no_version_tag
        st.subheader(f"No version tag in product-metadata ({len(no_tag)})")
        st.caption(
            "These descriptions carry no `Current version:` line, so the published version can "
            "never be read back — distributing will not change that. Add the line in "
            "product-metadata to bring them into the comparison."
        )
        if len(no_tag):
            st.dataframe(
                no_tag[["product", "dataset", "destination_id", "bytes_version"]],
                width="stretch",
                hide_index=True,
            )

    st.subheader("Helpful links")
    st.markdown(
        body="""
        - Github action to distribute from Bytes to Open Data: https://github.com/NYCPlanning/data-engineering/actions/workflows/distribute_socrata_from_bytes.yml
        - Open Data page to sign in and publish revisions: https://opendata.cityofnewyork.us/
        - Product Metadata repo: https://github.com/NYCPlanning/product-metadata
    """
    )
