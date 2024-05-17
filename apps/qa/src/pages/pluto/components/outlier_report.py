import streamlit as st
import pandas as pd
from typing import Callable
from st_aggrid import AgGrid


class OutlierReport:
    def __init__(self, data, v, v_prev, condo, mapped):
        self.df = data
        self.v = v
        self.v_prev = v_prev
        self.condo = condo
        self.mapped = mapped

    def __call__(self):
        st.header("Outlier Analysis")

        if self.v not in self.versions:
            st.info("There is no outlier report available for selected version.")
            return

        self.building_area_increase()

        self.small_apartments()

        self.floors()

    def display_dataframe(
        self, field: str, df: pd.DataFrame, filter: Callable | None = None
    ):
        st.markdown(self.markdown_dict[field])
        st.info(self.info_dict[field])

        if df.empty:
            st.write("There are no outliers for this check.")
        else:
            st.write(f"There are {df.shape[0]} outliers in total.")
            if filter:
                df = filter(df)

            with st.expander("Show table"):
                AgGrid(df)

    def building_area_increase(self):
        field = "building_area_increase"
        df = self.fetch_dataframe(field)
        self.display_dataframe(field, df)

    def small_apartments(self):
        field = "unitsres_resarea"
        df = self.fetch_dataframe(field)

        def filter(_df: pd.DataFrame):
            valid = "ownername" in df.columns
            help = (
                "This feature was implemented after this build" if not valid else None
            )
            if st.checkbox("Filter out NYCHA records", disabled=not valid, help=help):
                return _df[~(_df["ownername"] == "NYC HOUSING AUTHORITY")]
            else:
                return _df

        self.display_dataframe(field, df, filter)

    def floors(self):
        field = "lotarea_numfloor"
        df = self.fetch_dataframe(field)

        def filter(_df: pd.DataFrame):
            valid = "new_flag" in df.columns
            help = (
                "This feature was implemented after this build" if not valid else None
            )
            if st.checkbox("Only display new entries", disabled=not valid, help=help):
                return _df[_df["new_flag"]]
            else:
                return _df

        self.display_dataframe(field, df, filter)

    def fetch_dataframe(self, field):
        records = [i["values"] for i in self.v_outlier_records if i["field"] == field][
            0
        ]

        if records:
            df = pd.DataFrame(records)

            if field == "building_area_increase":
                df = df.drop(columns=["pair"])

            df["bbl"] = pd.to_numeric(df["bbl"], downcast="integer")
            return df
        else:
            return pd.DataFrame()

    @property
    def versions(self):
        return self.df.v.unique()

    @property
    def outlier_records(self):
        return self.df.loc[
            (self.df.condo == self.condo)
            & (self.df.mapped == self.mapped)
            & (self.df.v == self.v),
            :,
        ].to_dict("records")

    @property
    def v_outlier_records(self):
        return [i["outlier"] for i in self.outlier_records if i["v"] == self.v][0]

    @property
    def version_pair(self):
        return f"{self.v}-{self.v_prev}"

    @property
    def markdown_dict(self):
        return {
            "building_area_increase": f"### Table of BBLs with Unreasonable Increase in Building Area {self.version_pair}",
            "unitsres_resarea": f"### Report of BBLs with buildings containing unreasonably small apartments",
            "lotarea_numfloor": f"### Table of BBLs where bldgarea/lotarea > numfloors*2",
        }

    @property
    def info_dict(self):
        return {
            "building_area_increase": "The table displays all BBLs where building area is more than doubled since previous version.",
            "unitsres_resarea": "The table displays all BBLs where unitsres is more than 50 and resarea is greater than 0 but the ratio of resarea:unitsres is less than 300.",
            "lotarea_numfloor": "The table displays all BBLs where the ratio of bldgarea:lotarea is more than twice numfloors.",
        }
