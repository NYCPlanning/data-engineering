import pandas as pd
from zipfile import ZipFile
import streamlit as st

from dcpy.connectors.edm import publishing


def unzip_csv(csv_filename: str, zipfile: ZipFile):
    with zipfile.open(csv_filename) as csv:
        return pd.read_csv(csv, true_values=["t"], false_values=["f"])


def read_csv(product, draft_or_publish, label, file, **kwargs):
    if draft_or_publish == "Draft":
        return publishing.read_draft_csv(product, label, file, **kwargs)
    else:
        return publishing.read_csv(product, label, file, **kwargs)


@st.cache_data(ttl=600)
def read_csv_cached(product, draft_or_publish, label, file, **kwargs):
    return read_csv(product, draft_or_publish, label, file, **kwargs)
