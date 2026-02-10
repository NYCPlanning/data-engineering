def colp():
    import streamlit as st
    from src.shared.components import build_outputs, sidebar

    from .components.agency_usetype_report import (
        RecordsByAgency,
        RecordsByAgencyUsetype,
        RecordsByUsetype,
    )
    from .components.geospatial_check import GeospatialCheck
    from .components.manual_correction_report import ManualCorrection
    from .components.outlier_report import OutlierReport
    from .components.usetype_version_comparison_report import (
        UsetypeVersionComparisonReport,
    )
    from .helpers import get_data

    st.title("City Owned and Leased Properties QAQC")
    product_key = sidebar.data_selection("db-colp")

    build_outputs.data_directory_link(product_key)

    st.markdown(
        body="""
        ### About COLP Database

        COLP is a list of uses on city owned and leased properties that includes geographic information as well as the type of use, agency and other related information. \
        The input data for COLP is the Integrated Property Information System (IPIS), a real estate database maintained by the Department of Citywide Administrative Services (DCAS).
        
        ### About QAQC

        The QAQC page is designed to highlight key scenarios that can indicate potential data issues in a COLP Database build.
        """
    )

    if not product_key:
        st.header("Select a version.")
    else:
        data = get_data(product_key)

        RecordsByAgency(records_by_agency=data["records_by_agency"])()
        RecordsByUsetype(records_by_usetype=data["records_by_usetype"])()
        RecordsByAgencyUsetype(
            records_by_agency_usetype=data["records_by_agency_usetype"]
        )()
        OutlierReport(data=data)()
        ManualCorrection(data=data)()
        GeospatialCheck(data=data)()

        usetype_changes = data["usetype_changes"]
        if usetype_changes is None:
            version_for_comparison = None
        else:
            version_for_comparison = st.sidebar.selectbox(
                "Select a Version for Comparison", usetype_changes.v_current.unique()
            )
        UsetypeVersionComparisonReport(
            usetype_changes=usetype_changes, version=version_for_comparison
        )()
