import streamlit as st
import pandas as pd
from src.constants import DATASET_NAMES
from src.digital_ocean_utils import get_latest_build_version
from src.components.sources_report import sources_report
from src.ztl.components.outputs_report import output_report

DATASET_REPO_URL = "https://github.com/NYCPlanning/db-zoningtaxlots"
REFERENCE_VESION = "2023/03/01"
STAGING_VERSION = None

pd.options.display.float_format = "{:.2f}%".format


def ztl():
    dataset = DATASET_NAMES["ztl"]

    st.title("Zoning Tax Lots QAQC")
    col1, col2, col3 = st.columns(3)
    col1.markdown(
        f"""[![Build]({DATASET_REPO_URL}/actions/workflows/build.yml/badge.svg)]({DATASET_REPO_URL}/actions/workflows/build.yml)"""
    )
    col2.markdown(f"latest build version: `{get_latest_build_version(dataset)}`")
    col3.markdown(f"[github repo]({DATASET_REPO_URL})")

    report_type = st.sidebar.radio(
        "Select a report type",
        (
            "Sources",
            "Outputs",
        ),
    )

    if report_type == "Sources":
        sources_report(
            dataset=dataset,
            reference_version=REFERENCE_VESION,
            staging_version=STAGING_VERSION,
        )
    elif report_type == "Outputs":
        output_report()
    else:
        raise KeyError(f"Invalid ZTL report type {report_type}")
