import streamlit as st

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.utils import s3
from shared.components import sidebar
from shared.components.build_outputs import (
    generate_geo_data,
    generate_maps,
    load_build_outputs,
    show_build_output,
    show_map,
)

# Template DB outputs have used three layouts over time: everything at the root (24Q*),
# dataset_files/ with source_data_versions.csv at the root (early 26v1 drafts), and
# dataset_files/ + attachments/ (current). Try the newest location first.
CSV_FILE_CANDIDATES = [
    ["dataset_files/templatedb.csv", "templatedb.csv"],
    ["attachments/source_data_versions.csv", "source_data_versions.csv"],
]


def _find_csv_files(product_key) -> list[str]:
    assert PUBLISHING_BUCKET, "PUBLISHING_BUCKET must be defined"
    found = []
    for candidates in CSV_FILE_CANDIDATES:
        existing = [
            path
            for path in candidates
            if s3.object_exists(PUBLISHING_BUCKET, f"{product_key.path}/{path}")
        ]
        # If none exist, keep the newest path so loading fails with a clear error
        found.append(existing[0] if existing else candidates[0])
    return found


def template():
    st.title("QA of Template DB")
    product_key = sidebar.data_selection("db-template")

    st.markdown(
        body="""
        #### [Product wiki](https://github.com/NYCPlanning/data-engineering/wiki/Product:-TDB)
        
        This QA page highlights key scenarios that can indicate potential data issues in a build.
        """
    )

    if not product_key:
        st.warning("No build version selected")
        return

    # Reset when the user picks a different build/draft/version
    if st.session_state.get("loaded_product_key") != product_key:
        st.session_state["build_outputs"] = {}
        st.session_state["generated_geo_data"] = False
        st.session_state["generated_maps"] = False
        st.session_state["loaded_product_key"] = product_key

    # Show data
    for build_output in st.session_state["build_outputs"]:
        show_build_output(build_output)
        show_map(build_output)

    st.divider()

    # Load all data
    if not st.session_state["build_outputs"]:
        if not st.button(
            key="load_data_button",
            label="⏭️ Load build outputs",
            use_container_width=True,
        ):
            st.stop()
        st.session_state["build_outputs"] = load_build_outputs(
            product_key, _find_csv_files(product_key)
        )
        st.rerun()

    st.button(
        key="load_data_button",
        label="🔄 Refresh page to reload build outputs",
        use_container_width=True,
        disabled=True,
    )

    # Generate GeoDataFrames
    if not st.session_state["generated_geo_data"]:
        if not st.button(
            key="generate_geo_data_button",
            label="⏭️ Generate GeoDataFrames",
            use_container_width=True,
        ):
            st.stop()
        st.session_state["build_outputs"] = generate_geo_data(
            st.session_state["build_outputs"]
        )
        st.session_state["generated_geo_data"] = True
        st.rerun()

    st.button(
        key="generate_geo_data_button",
        label="🔄 Refresh page to regenerate GeoDataFrames",
        use_container_width=True,
        disabled=True,
    )

    # Generate maps
    if not st.session_state["generated_maps"]:
        st.warning(
            "Generating interactive maps with >10,000 geometries may take a few minutes"
        )
        if not st.button(
            key="generate_maps_button",
            label="⏭️ Generate maps",
            use_container_width=True,
        ):
            st.stop()
        st.session_state["build_outputs"] = generate_maps(
            st.session_state["build_outputs"]
        )
        st.session_state["generated_maps"] = True
        st.rerun()

    st.button(
        key="generate_maps_button",
        label="🔄 Refresh page to regenerate maps",
        use_container_width=True,
        disabled=True,
    )
