import pandas as pd
import plotly.express as px
import streamlit as st
from src.shared.constants import COLOR_SCHEME


class FieldDistributionReport:
    def __init__(self, data) -> None:
        self.qaqc_field_distribution = data["qaqc_field_distribution"]

    def __call__(self):
        st.subheader("Field Distributions")
        st.markdown(
            "These graphs show the distributions of fields for all records created or updated since the last version (based on the lastupdatedt field)."
        )

        if self.qaqc_field_distribution is None:
            st.info("Field Distribution report missing for this branch.")
            return

        for field_name, field_descriptions in self.report_sections().items():
            st.markdown(f"#### {field_descriptions['title']}")
            st.markdown(field_descriptions["description"])

            df = self.records_to_df(field_name)

            self.display_field_distribution_graph(
                df, field_name.lower(), field_descriptions["title"]
            )

    def display_field_distribution_graph(self, df, field_name, title):
        fig = px.bar(
            df,
            x=field_name,
            y="count",
            color_discrete_sequence=COLOR_SCHEME,
            labels={
                "count": "Count of Records",
                field_name: title,
            },
            title=f"{title} Distribution",
        )

        st.plotly_chart(fig)

    def records_to_df(self, field_name):
        return pd.DataFrame.from_records(
            self.qaqc_field_distribution.loc[
                self.qaqc_field_distribution.field_name == field_name
            ].iloc[0]["result"]
        )

    def report_sections(self):
        return {
            "Job_Status": {
                "title": "Job Status",
                "description": """
                DCP recode of DOB's status label. This describes the status of the job at the date of the data vintage. For example, a job marked as "3. Permitted" was at that status as of June 30, 2020 if using version 20Q2 of the DCP Housing Database. More details on each DOB status is available here:
                (https://www1.nyc.gov/assets/buildings/pdf/bisjobstatus.pdf)
                Jobs typically move through status A through X over time as they reach certain approval milestones:

                1. **Filed**: job application is at status A - G at the time of publication. Application submitted, but review is not yet in progress.

                2. **Plan Examination**: application is at status H - P. Plan examination is in progress, but not yet approved.

                3. **Permitted**: application is at status Q and R and may begin construction.
                
                4. **Partial Complete**: application at status U and X, and CO issued for NB or A1 job type, and the CO is a Temporary CO AND less than 80% of the units are completed for a building with 20 or more units.
                
                5. **Complete**: For new buildings and alterations, application is at status U and X, or a CO has been issued. For demolitions, the application is at status X. DCP has decided to mark demolitions as complete when they reach status X, but list the completion date as equal to status Q because this is likely when the building must be vacated, and it appears that many buildings are physically demolished some time before receiving sign off (status X).
                
                9. **Withdrawn**: application is at status 3. The application has been withdrawn by the applicant.
            """,
            },
            "Job_Type": {
                "title": "Job Type",
                "description": """
                DOB's type category for the job application. More information is available here. The following types are included in this database:

                1. **New Building (NB)**: an application to build a new structure. “NB” cannot be selected if any existing building elements are to remain—for example a part of an old foundation, a portion of a façade that will be incorporated into the construction, etc.

                2. **Alteration Type I (A1)**: a major alteration that will change the use, egress, or occupancy of the building.

                3. **Demolition (DM)**: an application to fully or partially demolish an existing building. Note that many demolition permits are only for partial demolitions and for garages (these are also captured).
            """,
            },
        }
