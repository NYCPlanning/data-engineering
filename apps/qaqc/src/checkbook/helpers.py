import pandas as pd
import streamlit as st


@st.cache_data(ttl=600)
def get_data():
    url = f"https://edm-publishing.nyc3.cdn.digitaloceanspaces.com/db-checkbook/checkbook-qaqc/2023-08-14/historical_spend.csv"
    df = pd.read_csv(url)

    return df
