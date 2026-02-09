import streamlit as st

SOURCE_NAME_MAPPINGS = {
    "dcp_edesignation": "Department of City Planning - E-Designations",
    "dcp_zoningmapindex": "Department of City Planning - Georeferenced NYC Zoning Maps",
    "dcp_colp": "Department of City Planning - NYC City Owned and Leased Properties",
    "dcp_zoningdistricts": "Department of City Planning - NYC GIS Zoning Features",
    "dcp_cdboundaries_wi": "Department of City Planning - Political and Administrative Districts",
    "dof_dtm": "Department of Finance - Digital Tax Map (DTM)",
    "pluto_input_cama_dof": "Department of Finance - Mass Appraisal System (CAMA)",
    "pluto_pts": "Department of Finance - Property Tax System (PTS)",
    "lpc_historic_districts": "Landmarks Preservation Commission - Historic Districts",
    "lpc_landmarks": "Landmarks Preservation Commission - Individual Landmarks",
    "pluto_input_numbldgs": "Office of Technology & Innovation – Building Footprint Centroids",  # this dataset is based on OTI building footprints
    "dpr_greenthumb": "Department of Parks and Recreation – GreenThumb Garden Info",
}


class SourceDataVersionsReport:
    def __init__(self, version_text):
        self.version_text = version_text

    def __call__(self):
        st.header("Source Data Versions")
        st.info(
            """
            ⚠️ This is only a subset of source datasets that have historically been a focus of QAQC.
            """
        )
        st.info(
            """
            ⚠️ Caution when referencing dates in dataset version values. These dates may not reflect when the data was generated.
            """
        )

        source_data_versions = self.version_text

        source_data_versions.rename(
            columns={
                "schema_name": "datalibrary_name",
                "v": "version",
            },
            errors="raise",
            inplace=True,
        )
        source_data_versions.set_index(
            "datalibrary_name",
            inplace=True,
        )

        source_data_versions = source_data_versions[
            source_data_versions.index.isin(list(SOURCE_NAME_MAPPINGS.keys()))
        ]
        source_data_versions.rename(
            index=SOURCE_NAME_MAPPINGS,
            inplace=True,
        )
        source_data_versions.index.names = ["dataset name"]

        source_data_versions.sort_values("dataset name", inplace=True)

        st.dataframe(source_data_versions)
