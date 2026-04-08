import streamlit as st


@st.cache_data(show_spinner=False, persist=True)
def get_versions():
    from dcpy.lifecycle.scripts import version_compare

    versions = version_compare.run()
    return versions
