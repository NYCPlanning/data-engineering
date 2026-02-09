import plotly.graph_objects as go
import streamlit as st
from src.shared.constants import COLOR_SCHEME


class QAQCVersionHistoryReport:
    def __init__(self, data, qaqc_check_dict, qaqc_check_sections) -> None:
        self.qaqc_version_history = data["qaqc_historic"]
        self.qaqc_checks = qaqc_check_dict
        self.qaqc_check_sections = qaqc_check_sections

    def __call__(self):
        st.subheader("Version History for QAQC Checks")
        st.markdown(
            """
            The following series of graphs highlight the changes between versions in the number of jobs flagged by a variety of QAQC Criteria. 
            The flags are broken down into sections that share a broad theme.
            To zoom in on a particular check, double click the check in the legend on the right of the graph.
            """
        )

        if self.qaqc_version_history is None:
            st.info("QAQC Version History file missing for this branch.")
            return

        for section_name, section_description in self.qaqc_check_sections.items():
            st.markdown(
                f"""
                #### {section_name} Checks
                {section_description}

                This section includes the following checks:
                """
            )
            self.display_check_distribution(section_name)

    def display_check_distribution(self, section):
        checks = self.checks_by_section(section)
        df = self.filter_by_checks(checks)

        fig = go.Figure()

        for check in checks:
            st.markdown(
                f"""
                 - {check}: {self.qaqc_checks[check]["description"]}
                """
            )
            fig.add_trace(self.generate_trace(check=check, df=df))

        fig.update_layout(template="plotly_white", colorway=COLOR_SCHEME)
        fig.update_xaxes(title_text="Version")
        fig.update_yaxes(title_text="Number of Jobs")
        st.plotly_chart(fig)

    def filter_by_checks(self, checks):
        return self.qaqc_version_history[checks + ["version"]]

    def checks_by_section(self, section):
        return [
            check
            for check, value in self.qaqc_checks.items()
            if value["section"] == section
        ]

    def generate_trace(self, check, df):
        return go.Scatter(
            x=df.version,
            y=df[check],
            mode="lines",
            name=check,
        )
