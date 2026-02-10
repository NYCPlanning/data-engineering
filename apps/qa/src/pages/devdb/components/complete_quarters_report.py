import plotly.express as px
import streamlit as st
from src.shared.constants import COLOR_SCHEME


class CompleteQuartersReport:
    def __init__(self, data):
        self.complete_quarters = data["qaqc_quarter_check"]

    def __call__(self):
        st.subheader("Complete Quarter Distribution")
        st.markdown(
            """
            This graph shows the distribution of records by complete quarter that were updated since the previous version.
            Complete Quarter represents the quarter that the job was completed. 
            For new buildings and alterations, this is defined as the quarter of the first certificate of occupancy issuance. 
            For demolitions, this is the quarter that the demolition was permitted (reached status Q).
            """
        )

        if self.complete_quarters is None:
            st.info("Complete Quarter Report file missing for this branch.")
            return

        self.display_complete_quarter_graph()

    def display_complete_quarter_graph(self):
        fig = px.bar(
            self.complete_quarters,
            x="complete_qrtr",
            y="count",
            labels={"complete_qrtr": "Completed Quarter", "count": "Count of Records"},
            color_discrete_sequence=COLOR_SCHEME,
        )
        st.plotly_chart(fig)
