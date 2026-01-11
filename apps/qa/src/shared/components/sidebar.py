import streamlit as st

from dcpy.connectors.edm import drafts, publishing


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
            select = st.sidebar.selectbox(label, options, key=f"{section_label}_output")
            if select:
                return publishing.BuildKey(product, select)
        case "Draft":
            label = "Select a version"
            options = drafts.get_draft_versions(product)
            version_select = st.sidebar.selectbox(
                label, options, key=f"{section_label}_version"
            )
            if version_select:
                draft_revision_options = drafts.get_draft_version_revisions(
                    product, version_select
                )
                draft_revision_label = "Select a draft"
                subversion_select = st.sidebar.selectbox(
                    draft_revision_label,
                    draft_revision_options,
                    key=f"{section_label}_output",
                )
                if subversion_select:
                    return publishing.DraftKey(
                        product, version_select, subversion_select
                    )
        case "Published":
            label = "Select a version"
            options = publishing.get_published_versions(
                product=product, exclude_latest=False
            )
            select = st.sidebar.selectbox(label, options, key=f"{section_label}_output")
            if select:
                return publishing.PublishKey(product, select)

    return None
