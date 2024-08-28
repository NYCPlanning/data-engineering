import pandas as pd
from zipfile import ZipFile
import streamlit as st

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.utils import s3
from dcpy.connectors.edm import publishing


def unzip_csv(csv_filename: str, zipfile: ZipFile) -> pd.DataFrame:
    with zipfile.open(csv_filename) as csv:
        return pd.read_csv(csv, true_values=["t"], false_values=["f"])


def read_file_metadata(
    product_key: publishing.ProductKey, filepath: str
) -> s3.Metadata:
    return s3.get_metadata(PUBLISHING_BUCKET, f"{product_key.path}/{filepath}")


@st.cache_data(ttl=600)
def read_csv_cached(
    product_key: publishing.ProductKey, filepath: str, **kwargs
) -> pd.DataFrame:
    return publishing.read_csv(product_key, filepath, **kwargs)
