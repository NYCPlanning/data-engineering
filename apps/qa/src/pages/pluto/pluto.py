def pluto():
    import streamlit as st

    from src.shared.components import sidebar
    from .helpers import get_data, PRODUCT
    from .components.version_comparison_report import version_comparison_report
    from .components.changes_report import ChangesReport

    st.title("PLUTO QAQC")

    product_key = sidebar.data_selection(PRODUCT)

    if not product_key:
        st.header("Select a version.")
    else:
        report_type = st.sidebar.selectbox(
            "Choose a Report Type",
            ["Compare with Previous Version", "Review Manual Changes"],
        )

        data = get_data(product_key)

        if report_type == "Compare with Previous Version":
            comp_type = st.sidebar.selectbox(
                "Comparison Type",
                ["Last of same version type", "Previous"],
                help="""
                    "Previous" will compare 24v2 - 24v1.1 diffs with 24v1.1 - 24v1 diffs \n
                    "Last of same version type" will compare instead to last major or minor i.e. 24v2 - 24v1.1 to 24v1 - 24v3.1
                """,
            )

            version_comparison_report(product_key, data, comp_type)

        elif report_type == "Review Manual Changes":
            ChangesReport(data)()
