import pandas as pd
import streamlit as st

from dcpy.connectors.edm import publishing
from dcpy.models.connectors.edm.publishing import BuildKey
from dcpy.utils import s3

PRODUCT = "db-cscl"
PROD_BUCKET = "edm-private"


@st.cache_data(show_spinner=False)
def get_builds() -> list[str]:
    return publishing.get_builds(PRODUCT)


@st.cache_data(show_spinner=False)
def get_build_version(build: str) -> str:
    product_key = BuildKey(PRODUCT, build)
    return publishing.get_build_metadata(product_key).version


@st.cache_data(show_spinner=False)
def get_diffs_summary(build: str) -> pd.DataFrame:
    product_key = BuildKey(PRODUCT, build)
    return publishing.read_csv(product_key, "validation_output/diffs_summary.csv")


@st.cache_data(show_spinner=False)
def get_diff_rows(build: str, filename: str) -> list[str]:
    """Stream dev and prod files from S3 and return lines present in dev but not prod.

    Equivalent to comm -23 <(sort dev) <(sort prod).
    Cached so repeated selections don't re-fetch from S3.
    """
    build_key = BuildKey(PRODUCT, build)
    prod_version = get_build_version(build)

    dev_buffer = publishing.get_file(build_key, filename)
    dev_lines = {
        line
        for line in dev_buffer.read().decode("latin-1").splitlines()
        if line.strip()
    }

    prod_buffer = s3.get_file_as_stream(
        PROD_BUCKET, f"cscl_etl/{prod_version}/{filename}"
    )
    prod_lines = {
        line
        for line in prod_buffer.read().decode("latin-1").splitlines()
        if line.strip()
    }

    return sorted(dev_lines - prod_lines)
