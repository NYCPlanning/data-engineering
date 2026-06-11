import streamlit as st

from dcpy.connectors.edm.models import BuildKey, DraftKey, ProductKey, PublishKey
from dcpy.lifecycle.builds import builds, drafts, published


def data_selection(product: str, section_label: str | None = None) -> ProductKey | None:
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
            options = builds.list_builds(product)
            select = st.sidebar.selectbox(label, options, key=f"{section_label}_output")
            if select:
                return BuildKey(product, select)
        case "Draft":
            label = "Select a version"
            options = drafts.get_dataset_versions(product)
            version_select = st.sidebar.selectbox(
                label, options, key=f"{section_label}_version"
            )
            if version_select:
                draft_revision_options = drafts.get_dataset_version_revisions(
                    product, version_select
                )
                draft_revision_label = "Select a draft"
                subversion_select = st.sidebar.selectbox(
                    draft_revision_label,
                    draft_revision_options,
                    key=f"{section_label}_output",
                )
                if subversion_select:
                    return DraftKey(product, version_select, subversion_select)
        case "Published":
            label = "Select a version"
            options = published.get_versions(product=product, exclude_latest=False)
            select = st.sidebar.selectbox(label, options, key=f"{section_label}_output")
            if select:
                return PublishKey(product, select)

    return None
