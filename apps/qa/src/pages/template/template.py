import streamlit as st
from src.shared.components import sidebar
from src.shared.components.build_outputs import (
    generate_geo_data,
    generate_maps,
    load_build_outputs,
    show_build_output,
    show_map,
)


def template():
    st.title("QA of Template DB")
    product_key = sidebar.data_selection("db-template")

    csv_files = [
        "templatedb.csv",
        "source_data_versions.csv",
    ]

    st.markdown(
        body="""
        #### [Product wiki](https://github.com/NYCPlanning/data-engineering/wiki/Product:-TDB)
        
        This QA page highlights key scenarios that can indicate potential data issues in a build.
        """
    )

    if not product_key:
        st.warning("No build version selected")
        return

    if "build_outputs" not in st.session_state:
        st.session_state["build_outputs"] = {}

    if "generated_geo_data" not in st.session_state:
        st.session_state["generated_geo_data"] = False

    if "generated_maps" not in st.session_state:
        st.session_state["generated_maps"] = False

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
        st.session_state["build_outputs"] = load_build_outputs(product_key, csv_files)
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
