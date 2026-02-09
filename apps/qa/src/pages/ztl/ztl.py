import pandas as pd
import streamlit as st
from src.shared.components import build_outputs, sidebar
from src.shared.components.sources_report import sources_report

from dcpy.connectors.edm import publishing

from .components.outputs_report import PRODUCT, output_report

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
    staging_product_key = sidebar.data_selection(PRODUCT, "Choose an output to QA")
    if not staging_product_key:
        st.header("Select a version.")
    else:
        build_outputs.data_directory_link(staging_product_key)

        if report_type == "Sources":
            reference_product_key = sidebar.data_selection(
                PRODUCT, "Choose a staging version"
            )
            if not reference_product_key:
                st.header("Select a reference version.")
            else:
                sources_report(
                    reference_product_key=reference_product_key,
                    staging_product_key=staging_product_key,
                )
        elif report_type == "Outputs":
            output_report(staging_product_key)
        else:
            raise KeyError(f"Invalid ZTL report type {report_type}")
