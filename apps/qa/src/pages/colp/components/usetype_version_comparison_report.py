import plotly.express as px
import streamlit as st
from src.shared.constants import COLOR_SCHEME


class UsetypeVersionComparisonReport:
    def __init__(self, usetype_changes, version):
        self.usetype_changes = usetype_changes
        self.version = version

    def __call__(self):
        st.subheader("Usetype Version Comparison")

        if self.usetype_changes is None:
            st.info("There is no usetype version comparison report for this branch.")
            return

        self.current_usetype_changes = self.filter_by_version(
            self.usetype_changes, self.version
        )

        st.markdown(
            f"""
            This report compares the usetype distribution between the previous and selected versions.
            Because there are a high number of usetypes, we have included a slider to be able to more clearly view a subsection.

            **Current Version:** {self.version}

            **Previous Version:** {self.get_previous_version(self.current_usetype_changes)}
            """
        )

        usetype_range = st.slider(
            "Select a range of values",
            min_value=0,
            max_value=self.current_usetype_changes.shape[0],
            value=[0, 12],
        )

        df = self.sort_and_filter_df(self.current_usetype_changes, usetype_range)

        self.display_graph(df, usetype_range)

    def display_graph(self, df, range):
        fig1 = px.bar(
            data_frame=df,
            y="usetype",
            x=["difference", "num_records_current", "num_records_previous"],
            barmode="group",
            color_discrete_sequence=COLOR_SCHEME,
            height=400 + (range[1] - range[0]) * 50,
            width=850,
            text_auto=True,
        )
        fig1.update_xaxes(title="Count of Records")
        fig1.update_yaxes(title="Use Type")
        fig1.update_layout(legend_title_text="Version")

        st.plotly_chart(fig1)

    def filter_by_version(self, df, version):
        return df.loc[df["v_current"] == version]

    def get_previous_version(self, df):
        return df.iloc[0]["v_previous"]

    def sort_and_filter_df(self, df, range):
        df = df.sort_values(by="num_records_current", ascending=False)
        df = df.iloc[range[0] : range[1]]

        return df.sort_values(by="num_records_current", ascending=True)
