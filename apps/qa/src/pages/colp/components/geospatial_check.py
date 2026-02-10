import json

import pandas as pd
import streamlit as st


class GeospatialCheck:
    def __init__(self, data):
        self.geospatial_check = data["geospatial_check"]

    def __call__(self):
        st.subheader("Geospatial Check")
        if self.geospatial_check is None:
            st.info("Geospatial Check Table Not Found")
            return
        df = self.geospatial_check
        df["result"] = df["result"].apply(json.loads)
        all_records = df.to_dict("records")
        self.display_not_in_nyc(all_records[0])
        self.display_nonmatch(all_records[1])

    def cast_version(self, v):
        if len(v) == 6:
            return str(v[:4]) + "-" + str(v[4:])

    def display_not_in_nyc(self, records):
        st.subheader("Properties That Are Not in NYC Borough Boundaries")
        st.markdown(
            "We want to check if there is any property that is located outside NYC Borough Boundaries (water included)."
        )
        records = records["result"][0]["values"]
        if records:
            df = pd.DataFrame(records)
            uid = df["uid"]
            df = df.drop(columns="uid")
            df = pd.concat([df, uid], axis=1)
            df["v"] = df["v"].apply(lambda x: self.cast_version(x))
            st.write(df)
        else:
            st.info("All properties are within NYC Borough Boundaries.")

    def display_nonmatch(self, records):
        st.subheader("Properties That Have Inconsistent Geographies")
        st.markdown(
            "We want to check whether the geocoded results lined up with other geography attributes in the data products.\
        The Borocode is checked against both Community District and BBL."
        )
        records = records["result"][0]["values"]
        if records:
            df = pd.DataFrame(records)
            uid = df["uid"]
            df = df.drop(columns="uid")
            df["v"] = df["v"].apply(lambda x: self.cast_version(x))
            df = pd.concat([df, uid], axis=1)
            st.write(df)
        else:
            st.info("No inconsistent geographies in the current version.")
