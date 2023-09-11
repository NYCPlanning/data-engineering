import pandas as pd
from zipfile import ZipFile
import streamlit as st

from dcpy.connectors.edm import publishing


def unzip_csv(csv_filename: str, zipfile: ZipFile):
    with zipfile.open(csv_filename) as csv:
        return pd.read_csv(csv, true_values=["t"], false_values=["f"])


@st.cache_data(ttl=600)
def read_csv_cached(product_key: publishing.ProductKey, file: str, **kwargs):
    return publishing.read_csv(product_key, file, **kwargs)
