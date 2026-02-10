import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from src.shared.components import build_outputs, sidebar
from src.shared.constants import COLOR_SCHEME

from .helpers import PRODUCT, get_latest_data


def facdb():
    st.title("Facilities DB QAQC")

    def plotly_table(df):
        fig = go.Figure(
            data=[
                go.Table(
                    header=dict(
                        values=list(df.columns),
                        line_color="darkslategray",
                        fill_color="gray",
                        font=dict(color="white", size=12),
                        align="center",
                    ),
                    cells=dict(
                        values=[df[i] for i in df.columns],
                        line_color="darkslategray",
                        fill_color="white",
                        align="left",
                    ),
                )
            ]
        )
        fig.update_layout(
            template="plotly_white",
            margin=go.layout.Margin(l=0, r=0, b=0, t=0),
            colorway=COLOR_SCHEME,
        )
        st.plotly_chart(fig)

    product_key = sidebar.data_selection(PRODUCT)

    if st.sidebar.button(
        label="Refresh data", help="Download newest files from Digital Ocean"
    ):
        st.cache_data.clear()

    general_or_classification = st.sidebar.selectbox(
        "Would you like to review general QAQC or changes by classification?",
        ("Review by classification level", "General review"),
    )

    if not product_key:
        st.header("Select a version.")
    else:
        build_outputs.data_directory_link(product_key)

        st.subheader(general_or_classification)

        qc_tables, qc_diff, qc_mapped = get_latest_data(product_key)

        def count_comparison(df, width=1000, height=1000):
            fig = go.Figure()
            for i in ["count_old", "count_new", "diff"]:
                fig.add_trace(go.Bar(y=df.index, x=df[i], name=i, orientation="h"))
            fig.update_layout(
                width=width,
                height=height,
                yaxis=dict(automargin=True),
                template="plotly_white",
                margin=go.layout.Margin(l=0, r=0, b=0, t=0),
                colorway=COLOR_SCHEME,
            )
            st.plotly_chart(fig)

        def geom_comparison(df, width=1000, height=600):
            fig = go.Figure()
            columns = [
                ("pct_mapped_old", "with_geom_old", "mapped in old:", "old"),
                ("pct_mapped_new", "with_geom_new", "mapped in new:", "new"),
                (
                    "mapped_pct_diff",
                    "mapped_diff",
                    "change in number of mapped units:",
                    "diff",
                ),
            ]
            for measure in columns:
                fig.add_trace(
                    go.Bar(
                        y=df.index,
                        x=df[measure[0]],
                        name=measure[3],
                        customdata=df[[measure[1], "count_new", "count_old"]],
                        hovertemplate=generate_hover(measure),
                        text=["{:.2%}".format(i) for i in df[measure[0]]],
                        orientation="h",
                    )
                )
            fig.update_layout(
                width=width,
                height=height,
                xaxis=dict(title="Percentage"),
                xaxis_tickformat=".2%",
                template="plotly_white",
                margin=go.layout.Margin(l=0, r=0, b=0, t=0),
                colorway=COLOR_SCHEME,
            )
            st.plotly_chart(fig, config=dict({"scrollZoom": True}))

        def generate_hover(measure):
            if measure[0] == "mapped_pct_diff":
                return measure[2] + " %{customdata[0]} <extra></extra>"
            elif measure[0] == "pct_mapped_old":
                return measure[2] + "%{customdata[0]}/%{customdata[2]} <extra></extra>"
            elif measure[0] == "pct_mapped_new":
                return measure[2] + "%{customdata[0]}/%{customdata[1]} <extra></extra>"

        def plot_diff_table(df, rows_to_style, as_pct=False):
            colors = px.colors.qualitative.Plotly
            to_display = df.copy()

            styled = (
                to_display.style.applymap(
                    lambda x: (
                        f"background-color: {colors[0]}"
                        if x
                        else "background-color: white"
                    ),
                    subset=rows_to_style[0:1],
                )
                .applymap(
                    lambda x: (
                        f"background-color: {colors[1]}"
                        if x
                        else "background-color: white"
                    ),
                    subset=rows_to_style[1:2],
                )
                .applymap(
                    lambda x: (
                        f"background-color: {colors[2]}"
                        if x
                        else "background-color: white"
                    ),
                    subset=rows_to_style[2:3],
                )
            )

            if as_pct:
                styled = styled.format({r: "{:.2%}".format for r in rows_to_style})

            st.dataframe(styled)

        def by_classification():
            """
            qc_diff visualization
            """
            level = st.sidebar.selectbox(
                "select a classification level",
                ["datasource", "factype", "facsubgrp", "facgroup", "facdomain"],
                index=0,
            )
            st.sidebar.success(
                """
                Use the dropdown input bar to select an attribute to review
                """
            )
            qc_diff_agg = qc_diff.groupby(level).sum()
            qc_diff_agg["diff_abs"] = qc_diff_agg["diff"].abs()
            qc_diff_agg["diff_abs_pct"] = (
                qc_diff_agg["diff_abs"] / qc_diff_agg["count_old"]
            ).round(3) * 100
            max_val = qc_diff_agg["diff"].max() - 1
            thresh = st.sidebar.number_input(
                "difference threshold",
                min_value=0,
                max_value=max_val,
                value=min(5, max_val),
                step=1,
            )

            st.sidebar.success(
                """
                Use the number input bar to change the difference
                threshold
                """
            )
            st.header(f"Change in total number of records by {level}")
            st.write(f"total change (positive or negative) > {thresh}")

            qc_diff_agg = qc_diff_agg.loc[
                (qc_diff_agg["diff"] != 0) & (~qc_diff_agg["diff"].isna()), :
            ]
            if level == "factype":
                st.warning(
                    "plot not available for this level,\
                    refer to the table below for more information"
                )
            else:
                count_comparison(
                    qc_diff_agg.loc[qc_diff_agg["diff"].abs() > thresh, :].sort_values(
                        "diff"
                    )
                )

            # st.header(f"Change in counts by {level}")
            qc_diff_agg.insert(0, level, qc_diff_agg.index)
            qc_diff_agg = qc_diff_agg.sort_values("diff", key=abs, ascending=False)

            plot_diff_table(qc_diff_agg, ["count_old", "count_new", "diff"])

            """
            qc_mapped visualization
            """
            st.header(f"Change in fraction of mapped records by {level}")
            st.write(
                """
                This section only reports instances where there is change in the percent
                of mapped records
            """
            )
            qc_map_agg = qc_mapped.groupby(level).sum()
            # qc_map_agg.insert(0, level, qc_map_agg.index)
            qc_map_agg["pct_mapped_old"] = (
                qc_map_agg["with_geom_old"] / qc_map_agg["count_old"]
            )
            qc_map_agg["pct_mapped_new"] = (
                qc_map_agg["with_geom_new"] / qc_map_agg["count_new"]
            )
            qc_map_agg["with_geom_old"] = qc_map_agg["with_geom_old"].round(2)
            qc_map_agg["with_geom_new"] = qc_map_agg["with_geom_new"].round(2)
            qc_map_agg["mapped_diff"] = (
                qc_map_agg["with_geom_new"] - qc_map_agg["with_geom_old"]
            )
            qc_map_agg["mapped_pct_diff"] = (
                qc_map_agg["pct_mapped_new"] - qc_map_agg["pct_mapped_old"]
            )
            qc_map_agg["mapped_pct_diff"] = qc_map_agg["mapped_pct_diff"].round(3)
            qc_map_agg = qc_map_agg[
                (
                    qc_map_agg["mapped_pct_diff"]
                    != 0 & ~qc_map_agg["mapped_pct_diff"].isna()
                )
            ]
            qc_map_agg.sort_values("mapped_pct_diff", ascending=True, inplace=True)
            geom_comparison(qc_map_agg)
            qc_map_agg = qc_map_agg[
                ["pct_mapped_old", "pct_mapped_new", "mapped_pct_diff"]
                + ["mapped_diff"]
                + list(qc_map_agg.columns[:-4])
            ]
            st.header(f"Percentage mapped records by {level}")
            plot_diff_table(
                qc_map_agg,
                ["pct_mapped_old", "pct_mapped_new", "mapped_pct_diff"],
                as_pct=True,
            )

        def general_review():
            st.header("New factypes")
            st.write("Facility types that do not appear in the previous FacDB")
            plotly_table(qc_diff.loc[qc_diff["count_old"] == 0, :])

            st.header("Old factypes (retired)")
            st.write(
                "Facility types that do appear in the previous FacDB, \
                but not in the latest version"
            )
            plotly_table(qc_diff.loc[qc_diff["count_new"] == 0, :])

            st.header("Full Panel Cross Version Comparison")
            st.write(
                "Reports the difference in the number of records at \
                the most micro level, which is the facility type and data source"
            )
            plotly_table(qc_diff)

            """
            important factypes
            """
            st.header("Changes in important factypes")
            st.write(
                "There should be little to no change in the \
                number of records with these facility types"
            )
            important_factype = [
                "FIREHOUSE",
                "POLICE STATION",
                "ACADEMIC LIBRARIES",
                "SPECIAL LIBRARIES",
                "EMERGENCY MEDICAL STATION",
                "HOSPITAL",
                "NURSING HOME",
                "ADULT DAY CARE",
                "SENIOR CENTER",
            ]
            important = (
                qc_diff.loc[qc_diff.factype.isin(important_factype), :]
                .groupby("factype")
                .sum()
            )
            count_comparison(important.sort_values("diff"), width=500, height=500)

            for key, value in qc_tables.items():
                st.header(key)
                if value["type"] == "dataframe":
                    plotly_table(value["dataframe"])
                else:
                    st.table(value["dataframe"])

        if general_or_classification == "General review":
            st.sidebar.success(
                "This option displays tables not specific to any classification level."
            )
            general_review()
        elif general_or_classification == "Review by classification level":
            st.sidebar.success(
                "This option displays info on change in total number of records and \
                change in number of records mapped"
            )
            by_classification()
