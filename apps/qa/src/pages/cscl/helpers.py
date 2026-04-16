import pandas as pd
import streamlit as st

from dcpy.connectors.edm import publishing

PRODUCT = "db-cscl"


@st.cache_data(show_spinner=False)
def get_builds() -> list[str]:
    return publishing.get_builds(PRODUCT)


@st.cache_data(show_spinner=False)
def get_build_version(build: str) -> str:
    product_key = publishing.BuildKey(PRODUCT, build)
    return publishing.get_build_metadata(product_key).version


@st.cache_data(show_spinner=False)
def get_diffs_summary(build: str) -> pd.DataFrame:
    product_key = publishing.BuildKey(PRODUCT, build)
    return publishing.read_csv(product_key, "validation_output/diffs_summary.csv")
