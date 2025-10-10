import pandas as pd
import streamlit as st

# from dcpy.utils.git import github
from dcpy.lifecycle.scripts.version_compare import run as compare_versions
## from src.shared.components.github import dispatch_workflow_button


def show_versions_streamlit_table(df: pd.DataFrame) -> None:
    st.write("## Distribution Status")

    for (product, dataset), row in df.iterrows():
        dataset_name = f"{product}.{dataset}"
        bytes_version = row["bytes_version"]
        open_version = row["open_data_versions"]

        cols = st.columns([4, 2, 3, 1, 1])
        cols[0].markdown(f"**{dataset_name}**")
        cols[1].write(bytes_version)
        cols[2].write(open_version)
        if bytes_version == open_version:
            cols[3].markdown(
                "<span style='color:green;font-size:20px'>&#10004;</span>",
                unsafe_allow_html=True,
            )
        else:
            cols[3].markdown(
                "<span style='color:red;font-size:20px'>&#10008;</span>",
                unsafe_allow_html=True,
            )
        # button doesn't do anything for now
        cols[4].button("Action", key=f"action_{row}")


def dashboard():
    show_versions_streamlit_table(compare_versions())
