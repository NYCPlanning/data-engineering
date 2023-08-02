from src.devdb.components.field_distribution_report import FieldDistributionReport


def devdb():
    import streamlit as st
    from src.report_utils import get_active_s3_folders
    from src.devdb.helpers import get_latest_data, REPO_NAME, BUCKET_NAME, QAQC_CHECK_DICTIONARY, QAQC_CHECK_SECTIONS
    from src.devdb.components.flagged_jobs_report import FlaggedJobsReport
    from src.devdb.components.qaqc_version_history_report import (
        QAQCVersionHistoryReport,
    )
    from src.devdb.components.complete_quarters_report import CompleteQuartersReport
    from src.components.sidebar import branch_selectbox

    st.title("Developments Database QAQC")
    st.markdown(
        body="""
        ### ABOUT DEVELOPMENTS DATABASE

        DCP’s Developments Database is the agency’s foundational dataset used for tracking construction and providing high accuracy estimates of current and near-term housing growth and analysis of housing trends. 
        It is the only source that can determine past and future change in Class A (permanent occupancy) and Class B (transient occupancy) housing units. 

        The NYC Department of City Planning’s (DCP) Housing Database Project-Level Files contain all NYC Department of Buildings (DOB)-approved housing construction and demolition jobs filed or completed in NYC since January 1, 2010. 
        It includes all three construction job types that add or remove residential units: new buildings, major alterations, and demolitions, and can be used to determine the change in legal housing units across time and space. 
        Records in the Housing Database Project-Level Files are geocoded to the greatest level of precision possible, subject to numerous quality assurance and control checks, recoded for usability, and joined to other housing data sources relevant to city planners and analysts.
    
        ### ABOUT QAQC

        The QAQC page is designed to highlight key scenarios that can indicate potential data issues in a Developments Database build.

        #### Additional Links
        - [DevDB Github Repo Wiki Page](https://github.com/NYCPlanning/db-developments/wiki)
        - [Medium Blog on DevDB](https://medium.com/nyc-planning-digital/introducing-dcps-housing-database-dcp-s-latest-open-data-product-b581aee97a51)
        """
    )

    branch = branch_selectbox(REPO_NAME, BUCKET_NAME)
    data = get_latest_data(branch)

    QAQCVersionHistoryReport(
        data=data,
        qaqc_check_dict=QAQC_CHECK_DICTIONARY,
        qaqc_check_sections=QAQC_CHECK_SECTIONS,
    )()

    FieldDistributionReport(data=data)()

    CompleteQuartersReport(data=data)()

    FlaggedJobsReport(data=data, qaqc_check_dict=QAQC_CHECK_DICTIONARY)()
