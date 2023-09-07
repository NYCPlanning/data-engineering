import streamlit as st
import pandas as pd

from dcpy.connectors.edm import publishing
from src.components.sources_report import sources_report
from src.components import sidebar
from src.ztl.components.outputs_report import output_report, PRODUCT

DATASET_REPO_URL = "https://github.com/NYCPlanning/data-engineering/"

pd.options.display.float_format = "{:.2f}%".format


def ztl():
    st.title("Zoning Tax Lots QAQC")
    col1, col2, col3 = st.columns(3)
    col1.markdown(
        f"""[![Build]({DATASET_REPO_URL}/actions/workflows/zoningtaxlots_build.yml/badge.svg)]({DATASET_REPO_URL}/actions/workflows/zoningtaxlots_build.yml)"""
    )
    col2.markdown(f"latest build version: `{publishing.get_latest_version(PRODUCT)}`")
    col3.markdown(f"[github repo]({DATASET_REPO_URL})")

    report_type = st.sidebar.radio(
        "Select a report type",
        (
            "Outputs",
            "Sources",
        ),
    )
    staging_type, staging_label = sidebar.data_selection(
        PRODUCT, "Choose an output to QA"
    )

    if report_type == "Sources":
        reference_type, reference_label = sidebar.data_selection(
            PRODUCT, "Choose a staging version"
        )
        sources_report(
            product=PRODUCT,
            reference_type=reference_type,
            reference_label=reference_label,
            staging_type=staging_type,
            staging_label=staging_label,
        )
    elif report_type == "Outputs":
        output_report(staging_type, staging_label)
    else:
        raise KeyError(f"Invalid ZTL report type {report_type}")
