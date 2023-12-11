import streamlit as st
from src.shared.constants import COLOR_SCHEME


def geometry_visualization_report(data: dict):
    st.header(f"Visualize Geometries")

    st.caption(
        f"""
        The intent of these maps is to guide engineers in figuring out if the shapefiles are corrupted or had been loaded improperly; indicators of this include points or polygons falling outside the NYC boundary, geometries appearing to be oversimplified (i.e. roadbeds look like "noodles"), and spatial data simply not existing.
        These maps are meant to be used in addition to the Mapped Capital Projects That Are Not in NYC table, as they may help identify which source spatial data files might be causing the issues. 
        Historically spatial data issues have been introduced when loading spatial files into data library, specifically when the source data projection has changed.
        If records are falling outside of NYC from a specific agency, that might indicate an issue upstream with how the data is being uploaded into data library
        """
    )

    st.pyplot(
        data["cpdb_dcpattributes_pts"].plot(markersize=5, color=COLOR_SCHEME[0]).figure
    )

    st.pyplot(data["cpdb_dcpattributes_poly"].plot(color=COLOR_SCHEME[0]).figure)
