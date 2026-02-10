from abc import ABC

import plotly.express as px
import streamlit as st
from src.shared.constants import COLOR_SCHEME


class CountRecordsReport(ABC):
    def __init__(self, data) -> None:
        self.content_to_display = False if data is None or data.empty else True

    def __call__(self):
        if not self.content_to_display:
            st.info(f"No Count of records by {self.y_axis_col}")
            return
        st.subheader(self.title, anchor=self.title_anchor)
        self.plot()

    def plot(self):
        slider_input = st.select_slider(
            label=f"Most common {self.category_plural} to visualize",
            options=range(1, self.data.shape[0] + 1),
            value=(1, 12),
        )
        self.data.sort_values(by="count", ascending=False, inplace=True)
        fig1 = px.bar(
            self.data.iloc[slider_input[0] - 1 : slider_input[1],].sort_values(
                by="count", ascending=True
            ),
            y=self.y_axis_col,
            x="count",
            orientation="h",
            barmode="group",
            width=850,
            height=500,
            color_discrete_sequence=COLOR_SCHEME,
        )

        fig1.update_yaxes(title=self.y_axis_label)
        fig1.update_layout(legend_title_text="Count")

        st.plotly_chart(fig1)


class RecordsByAgency(CountRecordsReport):
    def __init__(self, records_by_agency) -> None:
        self.by_agency = True
        self.y_axis_col = "AGENCY"
        self.y_axis_label = "Agency"
        self.by_usetype = False
        self.category_plural = "Agencies"
        self.title = "City owned and leased properties by agency"
        self.title_anchor = "count_by_agency"
        super().__init__(records_by_agency)
        if self.content_to_display:
            self.data = records_by_agency


class RecordsByUsetype(CountRecordsReport):
    def __init__(self, records_by_usetype) -> None:
        self.by_agency = False
        self.y_axis_col = "USETYPE"
        self.y_axis_label = "Use type"
        self.by_usetype = True
        self.category_plural = "Use types"
        self.title = "City owned and leased properties by usetype"
        self.title_anchor = "count_by_usetype"
        super().__init__(records_by_usetype)
        if self.content_to_display:
            self.data = records_by_usetype


class RecordsByAgencyUsetype(CountRecordsReport):
    def __init__(self, records_by_agency_usetype) -> None:
        self.by_agency = True
        self.y_axis_col = "agency-use type"
        self.y_axis_label = "Agency/use type combination"
        self.by_usetype = True
        self.title = "City owned and leased properties by agency and usetype"
        self.title_anchor = "count_by_agency_usetype"
        self.category_plural = "Agency-use type combinations"
        super().__init__(records_by_agency_usetype)
        if self.content_to_display:
            self.data = records_by_agency_usetype
            self.data["agency-use type"] = (
                self.data["AGENCY"] + " - " + self.data["USETYPE"].str.replace("-", "/")
            )
