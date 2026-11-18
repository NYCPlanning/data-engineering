import streamlit as st


def home():
    st.sidebar.success("Select a dataset above.")
    st.markdown(
        """
    <h1 style="font-size:3rem;">📊 Data Engineering QAQC</h1>
    """,
        unsafe_allow_html=True,
    )
    st.info(
        """
    Welcome to the QAQC Portal 🎉! This is a one stop shop for
    core **Data Engineering** data products, including PLUTO, Zoning Tax Lots,
    The Facilities database and more.

    You can select a dataset page through the dropdown menu on the left.
    If you have questions, email us at DataEngineering_DL@planning.nyc.gov
    or open up an [issue](https://github.com/NYCPlanning/data-engineering/issues) on Github.
    """
    )
