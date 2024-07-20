import streamlit as st

from dcpy.connectors.edm import publishing


def data_selection(
    product: str, section_label: str | None = None
) -> publishing.ProductKey | None:
    if section_label is not None:
        st.sidebar.title(section_label)
    product_type = st.sidebar.selectbox(
        "Published version, draft, or build?",
        ["Published", "Draft", "Build"],
        key=f"{section_label}_type",
    )

    match product_type:
        case "Build":
            label = "Select a build"
            options = publishing.get_builds(product)
        case "Draft":
            label = "Select a build"
            options = publishing.get_draft_builds(product)
        case "Published":
            label = "Select a version"
            options = publishing.get_published_versions(product)

    select = st.sidebar.selectbox(label, options, key=f"{section_label}_output")
    if select:
        match product_type:
            case "Build":
                return publishing.BuildKey(product, select)
            case "Draft":
                return publishing.DraftKey(product, select)
            case "Published":
                return publishing.PublishKey(product, select)
    else:
        return None
