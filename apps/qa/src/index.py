import importlib

import streamlit as st

from src.shared.constants import PAGES


def run():
    st.set_page_config(
        page_title="Data Engineering QAQC", page_icon="📊", layout="wide"
    )
    st.sidebar.markdown(
        """
        <div stule="margin-left: auto; margin-right: auto;">
        <img style='width:40%; margin: 0 auto 2rem auto;display:block;'
            src="https://raw.githubusercontent.com/NYCPlanning/logo/master/dcp_logo_772.png">
        </div>
        """,
        unsafe_allow_html=True,
    )

    datasets_list = list(PAGES.keys())
    query_params = st.query_params.to_dict()

    if query_params:
        name = query_params["page"]
    else:
        name = "Home"

    name = st.sidebar.selectbox(
        "Select a dataset for qaqc", datasets_list, index=datasets_list.index(name)
    )
    st.sidebar.divider()

    st.query_params["page"] = datasets_list[datasets_list.index(name)]

    dataset_module = importlib.import_module(f"pages.{PAGES[name]}.{PAGES[name]}")
    dataset_page = getattr(dataset_module, PAGES[name])
    dataset_page()


if __name__ == "__main__":
    run()
