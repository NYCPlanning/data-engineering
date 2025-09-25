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

    def display_dataframe(
        self, field: str, df: pd.DataFrame, filter: Callable | None = None
    ):
        st.markdown(self.markdown_dict[field])
        st.info(self.info_dict[field])

        if df.empty:
            st.write("There are no outliers for this check.")
        else:
            if filter:
                df = filter(df)
            st.write(f"There are {df.shape[0]} outliers in total.")

            # Add download button
            csv_data = df.to_csv(index=False)  # data string
            st.download_button(
                label="Download csv",
                data=csv_data,
                file_name=f"{field}.csv",
                mime="text/csv",
            )

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

    def fetch_dataframe(self, field):
        records = [i["values"] for i in self.v_outlier_records if i["field"] == field][
            0
        ]

        if records:
            df = pd.DataFrame(records)

            if field == "building_area_increase":
                df = df.drop(columns=["pair"])
                df["building_area_change"] = (
                    df["building_area_current"] - df["building_area_previous"]
                )

            # round values to integer in numeric-like columns
            for col in df.columns:
                if col != "bbl":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(int)
                else:
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
            "unitsres_resarea": "### Report of BBLs with buildings containing unreasonably small apartments",
        }

    @property
    def info_dict(self):
        return {
            "building_area_increase": """
                The table displays all BBLs where building area is more than doubled since previous version.

                ⚠️ Currently, there is no QA guidance for this field. These records can be safely ignored by the DE & GIS teams and and are not subject to manual correction.
            """,
            "unitsres_resarea": """
                The table displays all BBLs where unitsres is more than 50 and resarea is greater than 0 but the ratio of resarea:unitsres is less than 300.

                ⚠️ Non-NYCHA BBLs with the following criteria must be researched (and corrected if needed) by the DE team before sending to GIS: 
                * res_unit_ratio < **160 sq.ft.**, OR
                * unitsres > **800+** units.
            """,
        }
