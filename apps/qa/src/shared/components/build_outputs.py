from dataclasses import dataclass, field

import geopandas as gpd
import leafmap.foliumap as lmf
import pandas as pd
import streamlit as st
from src.shared.utils.publishing import read_csv_cached, read_file_metadata

from dcpy.connectors.edm import publishing
from dcpy.models.file import Geometry
from dcpy.models.geospatial import geometry
from dcpy.utils.geospatial import mapping, transform


def data_directory_link(product_key: publishing.ProductKey):
    data_url = publishing.get_data_directory_url(product_key)
    st.markdown(
        f"""
        [Link]({data_url}) to the dataset in Digital Ocean.        
        *Please let the DE team know if the link doesn't work.*
    """
    )


@dataclass
class BuildOutput:
    file_name: str
    metadata: dict
    dataframe: pd.DataFrame | None = None
    geodataframe: gpd.GeoDataFrame | None = None
    geodataframe_for_display: gpd.GeoDataFrame | None = None
    map: lmf.Map | None = None
    qa_warnings: list[str] = field(default_factory=list)


def load_build_outputs(product_key, csv_files) -> list[BuildOutput]:
    loaded_data = []
    for file_name in csv_files:
        with st.spinner(f"Loading csv file `{file_name}` to DataFrame ..."):
            metadata = read_file_metadata(product_key, file_name).model_dump()
            df = read_csv_cached(product_key, file_name)
        loaded_data.append(
            BuildOutput(
                file_name=file_name,
                metadata=metadata,
                dataframe=df,
            )
        )
    return loaded_data


def generate_geo_data(build_outputs: list[BuildOutput]) -> list[BuildOutput]:
    for build_output in build_outputs:
        # TODO generalize to work for files with different geometry formats and column names
        geometry_format = geometry.StandardGeometryFormat.wkb
        geometry_column = "wkb_geometry"
        with st.spinner(f"Generating `{build_output.file_name}` GeoDataFrame ..."):
            if build_output.dataframe is None:
                build_output.geodataframe = None
                build_output.qa_warnings.append(
                    "No DataFrame to generate GeoDataFrame from."
                )
            elif geometry_column not in build_output.dataframe.columns:
                build_output.geodataframe = None
                build_output.qa_warnings.append(f"Column {geometry_column} not found.")
            else:
                geom = Geometry(
                    geom_column=geometry_column, format=geometry_format, crs="EPSG:4326"
                )
                build_output.geodataframe = transform.df_to_gdf(
                    build_output.dataframe, geom
                )

                build_output.geodataframe_for_display = build_output.geodataframe.copy()
                # st.dataframe fails if a column is type gpd.GeoSeries
                astype_str = pd.Series(
                    build_output.geodataframe_for_display["geometry_generated"],
                    dtype="string",
                )
                build_output.geodataframe_for_display["geometry_generated"] = astype_str
    return build_outputs


def generate_maps(build_outputs: list[BuildOutput]) -> list[BuildOutput]:
    for build_output in build_outputs:
        if build_output.geodataframe is None:
            build_output.map = None
            build_output.qa_warnings.append("No GeoDataFrame to generate map from.")
        else:
            with st.spinner(f"Generating `{build_output.file_name}` map ..."):
                build_output.map = mapping.generate_folium_map(
                    build_output.geodataframe
                )

    return build_outputs


def show_build_output(build_output: BuildOutput) -> None:
    st.markdown(f"### {build_output.file_name}")
    df = build_output.dataframe
    gdf = build_output.geodataframe
    gdf_for_display = build_output.geodataframe_for_display

    st.markdown("##### S3 file metadata")
    st.json(build_output.metadata, expanded=False)

    st.markdown("##### DataFrame")
    if df is None:
        st.info("No DataFrame")
    else:
        st.markdown(
            f"""
            - Shape: {df.shape}
            """
        )
        st.dataframe(df, use_container_width=True)

    if gdf is None:
        st.info("No GeoDataFrame")
    else:
        st.markdown("##### GeoDataFrame")

        st.markdown(
            f"""
            - Shape: {gdf.shape}
            - Projection: {gdf.crs}
            - Geometry type counts:
            """
        )
        st.dataframe(gdf.geom_type.value_counts())
        st.dataframe(gdf_for_display, use_container_width=True)

    if build_output.qa_warnings:
        [st.warning(warning) for warning in build_output.qa_warnings]


def show_map(build_output: BuildOutput) -> None:
    map = build_output.map
    if map is None:
        st.info("No map")
        return

    map.to_streamlit(**mapping.DEFAULT_MAP_SIZE)
