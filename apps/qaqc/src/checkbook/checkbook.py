import streamlit as st
import pandas as pd
from helpers import get_data


def get_category_data(df):
    category_data = df.groupby("final_category", as_index=False).sum()
    return category_data


def get_geometry_data(df):
    geometry_data = df.groupby("has_geometry", as_index=False).sum()
    return geometry_data


data = get_data()


def checkbook():
    st.title("Historical Liquidations Visualizations")
    st.markdown(
        body="""
        ### About the Historical Liquidations Database

        The Historical Liquidations Dataset (db-checkbook), a data product produced by the New York City (NYC) Department of City Planning (DCP) Data Engineering team, presents the first-ever spatialized view of historical liquidations of the NYC capital budget. 
        Each row in the dataset corresponds to a unique capital project and captures vital information such as the sum of all checks disbursed for the project, a list of agencies associated with the project, a category assignment based on keywords in project text fields (‘Fixed Asset’, ‘Lump Sum’, or ‘ITT, Vehicles, and Equipment’), and most importantly, geospatial information associated with the project when possible. 
        Currently, the dataset is created from two source datasets: Checkbook NYC and the Capital Projects Database (CPDB). Checkbook NYC provides the information related to historical liquidations of the capital budget at the capital project level, while CPDB provides geospatial information for those projects. 
        Checkbook NYC is an open-source dataset and tool from the NYC Comptroller’s Office that publishes every check disbursed by the city. For the Historical Liquidations dataset, we limited the scope of this data source to only those checks pertaining to capital spending, as defined by Checkbook NYC. 
        CPDB is a data product produced by the NYC DCP Data Engineering team that “captures key data points on potential, planned, and ongoing capital projects sponsored or maintained by a capital agency in and around NYC” [source]. CPDB is updated three times per year in response to the Capital Commitment Plan (CCP). 
        Because only capital projects reported in the current fiscal year (FY)’s CCP are reflected in CPDB, the Historical Liquidations dataset utilizes the previous adopted versions of CPDB, in addition to the most recent version of CPDB from the current FY. 
        """
    )

    st.dataframe(data)

    grouped_by_category_all = get_category_data(data)
    grouped_by_geometry_all = get_geometry_data(data)

    st.header(
            "Check Amounts Per Category"
        )
    st.bar_chart(grouped_by_category_all, x = 'final_category', y = 'check_amount')

    st.header("Check Amounts Per Category")
    st.bar_chart(grouped_by_category_all, x="final_category", y="check_amount")

    agency = st.selectbox(
        "Agency Selection: ",
        (
            "SCHOOL CONSTRUCTION AUTHORITY",
            "Department of Education",
            "Transit Authority",
            "Department of Environmental Protection",
            "Housing, Preservation and Development",
            "Department of Parks and Recreation",
            "Department of Citywide Administrative Services",
            "Health and Hospitals Corporation",
        ),
        key="category",
    )

    agency_data = data[data["agency"].str.contains(agency) == True]
    grouped_by_category = agency_data.groupby("final_category", as_index=False).sum()

    st.bar_chart(grouped_by_category, x="final_category", y="check_amount")

    st.header("Check Amounts Represented by Geometries")
    st.bar_chart(grouped_by_geometry_all, x="has_geometry", y="check_amount")

    agency_geom = st.selectbox(
        "Agency Selection: ",
        (
            "SCHOOL CONSTRUCTION AUTHORITY",
            "Department of Education",
            "Transit Authority",
            "Department of Environmental Protection",
            "Housing, Preservation and Development",
            "Department of Parks and Recreation",
            "Department of Citywide Administrative Services",
            "Health and Hospitals Corporation",
        ),
        key="geometry",
    )

<<<<<<< HEAD
    agency_geom = st.selectbox('Agency Selection: ',
            ('SCHOOL CONSTRUCTION AUTHORITY', 
            'Department of Education', 
            'Transit Authority',
            'Department of Environmental Protection',
            'Housing, Preservation and Development',
            'Department of Parks and Recreation',
            'Department of Citywide Administrative Services',
            'Health and Hospitals Corporation'), key = 'geometry')

    agency_data_geom = data[data['agency'].str.contains(agency_geom) == True]
    grouped_by_agency_geom = agency_data_geom.groupby('has_geometry', as_index = False).sum()
    st.bar_chart(grouped_by_agency_geom, x = 'has_geometry', y = 'check_amount')
    return 
>>>>>>> 1a6e68b (changes to charts)
=======
    agency_data_geom = data[data["agency"].str.contains(agency_geom) == True]
    grouped_by_agency_geom = agency_data_geom.groupby(
        "has_geometry", as_index=False
    ).sum()
    st.bar_chart(grouped_by_agency_geom, x="has_geometry", y="check_amount")
    return
>>>>>>> 31504ad (reformatting qaqc checkbook)
