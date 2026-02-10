import json

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid


def withinNYC_check(data):
    st.header("Mapped Capital Projects That Are Not in NYC")
    st.markdown(
        """
        We check whether all mapped capital projects are located within the NYC borough boundaries (water included).
        """
    )

    def fetch_dataframe(geo_check_records, field):
        records = [i["values"] for i in geo_check_records if i["field"] == field][0]
        if records:
            df = pd.DataFrame(records)
            return df
        else:
            return pd.DataFrame()

    df = data["geospatial_check"]
    df["result"] = df["result"].apply(json.loads)
    geo_check_records = df.to_dict("records")
    if not geo_check_records:
        st.write("No such projects.")
    else:
        geo_check = [i["result"] for i in geo_check_records][0]
        df = fetch_dataframe(geo_check, "projects_not_within_NYC")
        if df.empty:
            st.write("No such projects.")
        else:
            count = df.shape[0]
            AgGrid(df)
            st.write(
                f"There are {count} mapped projects that are not within the NYC borough boundaries water included."
            )
