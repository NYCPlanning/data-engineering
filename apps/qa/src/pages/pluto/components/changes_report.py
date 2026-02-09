from abc import ABC

import numpy as np
import plotly.express as px
import streamlit as st
from src.shared.constants import COLOR_SCHEME
from st_aggrid import AgGrid


class ChangesReport:
    def __init__(self, data) -> None:
        self.applied_changes = data["pluto_changes_applied"]
        self.not_applied_changes = data["pluto_changes_not_applied"]
        self.version_dropdown = np.flip(
            np.sort(data["pluto_changes_applied"].version.dropna().unique())
        )

    def __call__(self):
        st.header("Manual Changes")

        st.markdown(
            """
            PLUTO is created using the best available data from a number of city agencies. To further
            improve data quality, the Department of City Planning (DCP) applies changes to selected field
            values.

            Each change to a field is labeled for a version of PLUTO.
            
            For programmatic changes, this is version in which the programmatic change was
            implemented. For research and user reported changes, this is the version in which the BBL
            change was added to PLUTO_input_research.csv.

            For more information about the structure of the pluto changes report,
            see the [Pluto Changelog Readme](https://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/pluto_change_file_readme.pdf?r=22v1).

            NOTE: This report is based on the files
            `pluto_changes_applied.csv`/`pluto_changes_not_applied.csv`
            (or legacy files `pluto_corrections_applied.csv`/`pluto_corrections_not_applied.csv`)
            """
        )

        if self.applied_changes is None or self.not_applied_changes is None:
            st.info(
                "There are no available changes reports for this branch. This is likely due to a problem on the backend with the files on Digital Ocean."
            )
            return

        version = st.sidebar.selectbox(
            "Filter the changes to fields by the PLUTO Version in which they were first introduced",
            self.version_dropdown,
        )

        AppliedChangesSection(self.applied_changes, version)()
        NotAppliedChangesSection(self.not_applied_changes, version)()

        st.info(
            """
            See [here](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-pluto-mappluto.page) for a full accounting of the changes made for the latest version
            in the PLUTO change file.
            """
        )


class ChangesSection(ABC):
    def __init__(self, changes, version) -> None:
        super().__init__()
        self.changes = self.filter_by_version(changes, version)
        self.version_text = self.get_version_text(version)

    def filter_by_version(self, df, version):
        if version == "All":
            return df
        else:
            return df.loc[df["version"] == version]

    def get_version_text(self, version):
        return "All Versions" if version == "All" else f"Version {version}"

    def display_changes_figures(self, df, title):
        figure = self.generate_graph(self.field_change_counts(df), title)
        st.plotly_chart(figure)

        self.display_changes_df(df, title)

    def generate_graph(self, changes, title):
        return px.bar(
            changes,
            x="field",
            y="size",
            text="size",
            title=title,
            labels={"size": "Count of Records", "field": "Altered Field"},
            color_discrete_sequence=COLOR_SCHEME,
        )

    def field_change_counts(self, df):
        return df.groupby(["field"]).size().to_frame("size").reset_index()

    def display_changes_df(self, changes, title):
        changes = changes.sort_values(
            by=["version", "reason", "bbl"], ascending=[False, True, True]
        )

        AgGrid(data=changes, key=f"display_changes_df_{title}")


class AppliedChangesSection(ChangesSection):
    def __call__(self):
        st.subheader("Manual Changes Applied", anchor="changes-applied")

        if self.changes.empty:
            st.info(f"No Changes introduced in {self.version_text} were applied.")
        else:
            title_text = (
                f"Applied Manual Changes introduced in {self.version_text} by Field"
            )
            self.display_changes_figures(self.changes, title_text)
        st.markdown(
            """
            For each record in the PLUTO Changes table, PLUTO attempts to change a record to the New Value column by matching on the BBL and the 
            Old Value column. The graph and table below outline the records in the pluto changes table that were successfully applied to PLUTO.
            """
        )


class NotAppliedChangesSection(ChangesSection):
    def __call__(self):
        st.subheader("Manual Changes Not Applied", anchor="changes-not-applied")
        st.markdown(
            """ 
            For each record in the PLUTO Changes table, PLUTO attempts to change a record by matching on the BBL and the 
            Old Value column. As the underlying datasources change and improve, PLUTO records may no longer match the old value 
            specified in the pluto changes table. The graph and table below outline the records in the pluto changes table that failed to be applied for this reason.
            """
        )

        if self.changes.empty:
            st.info(f"All Changes introduced in {self.version_text} were applied.")
        else:
            title_text = (
                f"Manual Changes not Applied introduced in {self.version_text} by Field"
            )
            self.display_changes_figures(self.changes, title_text)
