import pandas as pd
import streamlit as st


def get_data():

    print('started load')

    url = f"https://edm-publishing.nyc3.cdn.digitaloceanspaces.com/db-checkbook/checkbook-qaqc/2023-08-14/historical_spend.csv"
    df = pd.read_csv(url)

    return df
