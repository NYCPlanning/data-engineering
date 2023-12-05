import pandas as pd
from zipfile import ZipFile
import streamlit as st

from dcpy.utils import s3
from dcpy.connectors.edm import publishing

from src.constants import BUCKET_NAME


def unzip_csv(csv_filename: str, zipfile: ZipFile):
    with zipfile.open(csv_filename) as csv:
        return pd.read_csv(csv, true_values=["t"], false_values=["f"])


def read_file_metadata(product_key: publishing.ProductKey, filepath: str):
    return s3.get_metadata(BUCKET_NAME, f"{product_key.path}/{filepath}")


@st.cache_data(ttl=600)
def read_csv_cached(product_key: publishing.ProductKey, filepath: str, **kwargs):
    return publishing.read_csv(product_key, filepath, **kwargs)
