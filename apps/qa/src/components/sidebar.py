import streamlit as st
from typing import Optional

from dcpy.connectors.edm import publishing


def data_selection(
    product: str, section_label: Optional[str] = None
) -> publishing.Product:
    if section_label is not None:
        st.sidebar.title(section_label)
    publish_or_draft = st.sidebar.selectbox(
        "Published version or draft?",
        ["Published", "Draft"],
        key=f"{section_label}_type",
    )
    is_draft = publish_or_draft == "Draft"
    if is_draft:
        label = "Select a build"
        options = publishing.get_draft_builds(product)
    else:
        label = "Select a version"
        options = publishing.get_published_versions(product)
    select = st.sidebar.selectbox(label, options, key=f"{section_label}_output")
    if is_draft:
        return publishing.Draft(product, select)
    else:
        return publishing.Product(product, select)
