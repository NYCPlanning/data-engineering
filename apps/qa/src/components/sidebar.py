import streamlit as st
from typing import Optional

from dcpy.connectors.edm import publishing


def data_selection(product: str, section_label: Optional[str] = None):
    if section_label is not None:
        st.sidebar.title(section_label)
    publish_or_draft = st.sidebar.selectbox(
        "Published version or draft?",
        ["Published", "Draft"],
        key=f"{section_label}_type",
    )
    if publish_or_draft == "Draft":
        label = "Select a build"
        options = publishing.get_draft_builds(product)
    else:
        label = "Select a version"
        options = publishing.get_published_versions(product)
    select = st.sidebar.selectbox(label, options, key=f"{section_label}_output")
    return publish_or_draft, select
