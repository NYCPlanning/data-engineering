"""
Resource manager for EDDE data sources.

This module centralizes loading of all source data files from the resources/ folder,
making it easier to update and maintain data sources.

Usage:
    from resources import load

    df = load("2010_census_housing_units_by_2020_nta")
"""

import pandas as pd

# Helper functions for loading resources


def _load_2010_census_housing_units(path: str):
    return pd.read_csv(
        path,
        dtype={"HUnits": int},
    )


def _load_decennial_census_001020(path: str):
    return pd.read_excel(
        path,
        skiprows=2,
        dtype={"GeogType": str, "GeoID": str},
    )


def _load_acs_0812(path: str):
    return pd.read_excel(
        path,
        dtype={"Geog": str},
    )


def _load_acs_1923(path: str):
    return pd.read_excel(
        path,
        dtype={"Geog": str},
    )


def _load_census_2000(path: str):
    return pd.read_excel(
        path,
        skiprows=1,
        dtype={"GeoID": str},
    )


def _load_education_outcome_data(path: str):
    return pd.read_excel(
        path,
        sheet_name="Data",
    )


def _load_education_outcome_data_dictionary(path: str):
    return pd.read_excel(
        path,
        sheet_name="Data Dictionary",
    )


def _load_pedestrian_hospitalizations(path: str):
    return pd.read_csv(
        path,
        dtype={"Geography": str},
    )

def _load_assault_hospitalizations(path: str):
    return pd.read_csv(
        path,
        dtype={"Geography": str},
    )


def _load_health_mortality_puma(path: str):
    return pd.read_excel(
        path,
        sheet_name="PUMA",
        header=1,
        nrows=55,
        dtype={"PUMA": str},
    )


def _load_health_mortality_borough(path: str):
    return pd.read_excel(
        path,
        sheet_name="Borough",
        header=1,
        nrows=5,
    )


def _load_health_mortality_citywide(path: str):
    return pd.read_excel(
        path,
        sheet_name="City",
        header=1,
        nrows=1,
    )


def _load_diabetes_self_report(path: str):
    return pd.read_excel(
        path,
        sheet_name="DCHP_Diabetes_SelfRepHealth",
    )


def _load_heat_vulnerability(path: str):
    return pd.read_excel(
        path,
        usecols=["PUMACE10", "HVI"],
        dtype={"PUMACE10": str, "HVI": int},
    )


def _load_covid_death(path: str):
    return pd.read_excel(
        path,
        sheet_name="Sheet 1",
    )


def _load_census_aggregations(path: str):
    return pd.read_csv(
        path,
        header=2,
    )


def _load_transportation_park_access(path: str):
    return pd.read_excel(
        path,
        sheet_name="Park_Qtr_Mile_Access",
        dtype={"PUMA": str},
    )


def _load_transportation_jobs_access(path: str):
    return pd.read_excel(
        path,
        sheet_name="Access_to_Jobs",
        dtype={"PUMA": str},
    )


def _load_transportation_subway_sbs_access(path: str):
    return pd.read_excel(
        path,
        sheet_name="Subway_SBS_Qr_Mile_Access",
        dtype={"PUMA": str},
    )


def _load_transportation_ada_subway_access(path: str):
    return pd.read_excel(
        path,
        sheet_name="ADA_Subway_Qtr_Mile_Access",
        dtype={"PUMA": str},
    )


def _load_nychvs_renter_occupied(path: str):
    return pd.read_excel(
        path,
        sheet_name="Renter-occupied housing units",
        dtype={"geo_id": str},
    )


def _load_nychvs_rent_stabilized(path: str):
    return pd.read_excel(
        path,
        sheet_name="Occupied rent stabilized",
        dtype={"geo_id": str},
    )


def _load_nychvs_occupied(path: str):
    return pd.read_excel(
        path,
        sheet_name="Occupied housing units",
        dtype={"geo_id": str},
    )


def _load_nychvs_three_plus_probs(path: str):
    return pd.read_excel(
        path,
        sheet_name="Occupied housing 3+ problems",
        dtype={"geo_id": str},
    )


def _load_eviction_filings(path: str):
    return pd.read_excel(
        path,
        skiprows=4,
        nrows=59,
    )


def _load_nycha_tenants(path: str):
    return pd.read_excel(
        path,
        sheet_name="PUMA",
    )


def _load_housing_lottery_applications(path: str):
    return pd.read_excel(
        path,
        dtype={"geog": str},
        sheet_name="housing_lottery_applications",
    )


def _load_housing_lottery_leases(path: str):
    return pd.read_excel(
        path,
        dtype={"geog": str},
        sheet_name="housing_lottery_leases",
    )


# Resource registry
RESOURCES = {
    # Housing Production
    "2010_census_housing_units_by_2020_nta": {
        "filepath": "resources/housing_production/2010_census_housing_units_by_2020_NTA.csv",
        "type": "csv",
        "data_table": "",
        "required_columns": ["HUnits", "GeoType", "Geog"],
        "loader": _load_2010_census_housing_units,
    },
    # Decennial Census
    "decennial_census_001020": {
        "filepath": "resources/decennial_census_data/EDDE_Census00-10-20_MUTU.xlsx",
        "type": "excel",
        "data_table": "",
        "required_columns": [
            "GeogType",
            "GeoID",
            "Pop20",
            "Pop10",
            "Pop00",
            "Hsp20",
            "WNH20",
            "BNH20",
            "ANH20",
            "OTwoNH20",
        ],
        "loader": _load_decennial_census_001020,
    },
    # ACS PUMS - specific year windows
    "acs_0812": {
        "filepath": "resources/ACS_PUMS/EDDE_ACS2008-2012.xlsx",
        "type": "excel",
        "data_table": "",
        "required_columns": ["Geog"],
        "loader": _load_acs_0812,
    },
    "acs_2024": {
        "filepath": "resources/ACS_PUMS/EDDE_ACS2020-2024.xlsx",
        "type": "excel",
        "data_table": "",
        "required_columns": ["Geog"],
        "loader": _load_acs_1923,
    },
    "census_2000": {
        "filepath": "resources/ACS_PUMS/EDDE_Census2000PUMS.xlsx",
        "type": "excel",
        "data_table": "",
        "required_columns": ["GeoID"],
        "loader": _load_census_2000,
    },
    # Quality of Life - Education
    "education_outcome_data": {
        "filepath": "resources/quality_of_life/education_math_ela_grad.xlsx",
        "type": "excel",
        "sheet_name": "Data",
        "data_table": "5.11,5.12,5.13",
        "required_columns": ["NTA Code", "NTA Name"],
        "loader": _load_education_outcome_data,
    },
    "education_outcome_data_dictionary": {
        "filepath": "resources/quality_of_life/education_math_ela_grad.xlsx",
        "type": "excel",
        "sheet_name": "Data Dictionary",
        "data_table": "5.11,5.12,5.13",
        "required_columns": ["varlabel", "varname"],
        "loader": _load_education_outcome_data_dictionary,
    },
    # Quality of Life - Safety
    "assault_hospitalizations": {
        "filepath": "resources/quality_of_life/non_fatal_assault_hospitalizations.csv",
        "type": "csv",
        "data_table": "",
        "required_columns": ["Geography", "Number", "GeoType"],
        "loader": _load_assault_hospitalizations,
    },
    "pedestrian_hospitalizations": {
        "filepath": "resources/quality_of_life/pedestrian_hospitalizations.csv",
        "type": "csv",
        "data_table": "",
        "required_columns": ["Geography", "Number", "GeoType"],
        "loader": _load_pedestrian_hospitalizations,
    },
    # Quality of Life - Health Mortality (multi-sheet)
    "health_mortality_puma": {
        "filepath": "resources/quality_of_life/dohmh_death_rate_and_overdose.xlsx",
        "type": "excel",
        "sheet_name": "PUMA",
        "data_table": "5.03,5.04,5.05",
        "required_columns": ["PUMA"],  # File missing - columns TBD
        "loader": _load_health_mortality_puma,
    },
    "health_mortality_borough": {
        "filepath": "resources/quality_of_life/dohmh_death_rate_and_overdose.xlsx",
        "type": "excel",
        "sheet_name": "Borough",
        "data_table": "5.03,5.04,5.05",
        "required_columns": ["Borough"],  # File missing - columns TBD
        "loader": _load_health_mortality_borough,
    },
    "health_mortality_citywide": {
        "filepath": "resources/quality_of_life/dohmh_death_rate_and_overdose.xlsx",
        "type": "excel",
        "sheet_name": "City",
        "data_table": "5.03,5.04,5.05",
        "required_columns": ["City"],  # File missing - columns TBD
        "loader": _load_health_mortality_citywide,
    },
    # Quality of Life - Diabetes
    "diabetes_self_report": {
        "filepath": "resources/quality_of_life/diabetes_self_report/diabetes_self_report_processed_2024.xlsx",
        "type": "excel",
        "sheet_name": "DCHP_Diabetes_SelfRepHealth",
        "data_table": "",
        "required_columns": [
            "ID",
            "Borough",
            "geo_type",
            "Diabetes",
            "Self_Rep_Health",
        ],
        "loader": _load_diabetes_self_report,
    },
    # Quality of Life - Heat Vulnerability
    "heat_vulnerability": {
        "filepath": "resources/quality_of_life/HVI_PUMA_Subboro_forSharing.xlsx",
        "type": "excel",
        "data_table": "",
        "required_columns": ["PUMACE10", "HVI"],  # File missing
        "loader": _load_heat_vulnerability,
    },
    # Quality of Life - COVID Death
    "covid_death": {
        "filepath": "resources/quality_of_life/deaths_by_race_and_puma.xlsx",
        "type": "excel",
        "sheet_name": "Sheet 1",
        "data_table": "5.06",
        "required_columns": ["PUMA", "Total\nDeaths", "Race/Ethnicity"],
        "loader": _load_covid_death,
    },
    "census_aggregations": {
        "filepath": "resources/quality_of_life/Census_Aggregations_fromErica.csv",
        "type": "csv",
        "data_table": "",
        "required_columns": [
            "GeogType",
            "GeoID",
            "ANH20",
            "BNH20",
            "Hsp20",
            "OTwoNH20",
            "WNH20",
        ],  # File missing
        "loader": _load_census_aggregations,
    },
    # Quality of Life - Transportation Access (multi-sheet)
    "transportation_park_access": {
        "filepath": "resources/quality_of_life/transportation.xlsx",
        "type": "excel",
        "sheet_name": "Park_Qtr_Mile_Access",
        "data_table": "5.09",
        "required_columns": ["PUMA", "Pop_Served", "Total_Pop"],
        "loader": _load_transportation_park_access,
    },
    "transportation_jobs_access": {
        "filepath": "resources/quality_of_life/transportation.xlsx",
        "type": "excel",
        "sheet_name": "Access_to_Jobs",
        "data_table": "5.08",
        "required_columns": [
            "PUMA",
            "Weighted Average Number of Jobs Accessible within 30 mins from Tract Centroid by Transit",
        ],
        "loader": _load_transportation_jobs_access,
    },
    "transportation_subway_sbs_access": {
        "filepath": "resources/quality_of_life/transportation.xlsx",
        "type": "excel",
        "sheet_name": "Subway_SBS_Qr_Mile_Access",
        "data_table": "5.09",
        "required_columns": [
            "PUMA",
            "Pop within 1/4 Mile of Subway Stations and SBS Stops",
            "Total_Pop",
        ],
        "loader": _load_transportation_subway_sbs_access,
    },
    "transportation_ada_subway_access": {
        "filepath": "resources/quality_of_life/transportation.xlsx",
        "type": "excel",
        "sheet_name": "ADA_Subway_Qtr_Mile_Access",
        "data_table": "5.09",
        "required_columns": [
            "PUMA",
            "Pop within 1/4 Mile of ADA Subway Stations",
            "Total_Pop",
        ],
        "loader": _load_transportation_ada_subway_access,
    },
    # Housing Security - NYCHVS (multi-sheet)
    "nychvs_renter_occupied": {
        "filepath": "resources/housing_security/nychvs_2023.xlsx",
        "type": "excel",
        "sheet_name": "Renter-occupied housing units",
        "data_table": "",
        "required_columns": ["geo_id", "geo_type"],
        "loader": _load_nychvs_renter_occupied,
    },
    "nychvs_rent_stabilized": {
        "filepath": "resources/housing_security/nychvs_2023.xlsx",
        "type": "excel",
        "sheet_name": "Occupied rent stabilized",
        "data_table": "",
        "required_columns": ["geo_id", "geo_type"],
        "loader": _load_nychvs_rent_stabilized,
    },
    "nychvs_occupied": {
        "filepath": "resources/housing_security/nychvs_2023.xlsx",
        "type": "excel",
        "sheet_name": "Occupied housing units",
        "data_table": "",
        "required_columns": ["geo_id", "geo_type"],
        "loader": _load_nychvs_occupied,
    },
    "nychvs_three_plus_probs": {
        "filepath": "resources/housing_security/nychvs_2023.xlsx",
        "type": "excel",
        "sheet_name": "Occupied housing 3+ problems",
        "data_table": "",
        "required_columns": ["geo_id", "geo_type"],
        "loader": _load_nychvs_three_plus_probs,
    },
    # Housing Security - Other
    "eviction_filings": {
        "filepath": "resources/housing_security/eviction_filings.xlsx",
        "type": "excel",
        "data_table": "",
        "required_columns": ["Community District", "Eviction Fillings*"],
        "loader": _load_eviction_filings,
    },
    "nycha_tenants": {
        "filepath": "resources/housing_security/nycha_tenants.xlsx",
        "type": "excel",
        "sheet_name": "PUMA",
        "data_table": "",
        "required_columns": ["PUMA (2020)", "Total Unit Count"],
        "loader": _load_nycha_tenants,
    },
    # Housing Security - HPD Housing Lottery (multi-sheet)
    "housing_lottery_applications": {
        "filepath": "resources/housing_security/hpd_housing_lottery.xlsx",
        "type": "excel",
        "sheet_name": "housing_lottery_applications",
        "data_table": "3.13",
        "required_columns": ["geog", "geo_type", "Total"],
        "loader": _load_housing_lottery_applications,
    },
    "housing_lottery_leases": {
        "filepath": "resources/housing_security/hpd_housing_lottery.xlsx",
        "type": "excel",
        "sheet_name": "housing_lottery_leases",
        "data_table": "3.14",
        "required_columns": ["geog", "geo_type", "Total"],
        "loader": _load_housing_lottery_leases,
    },
}


def load(resource_name: str) -> pd.DataFrame:
    """
    Load a resource by name.

    Args:
        resource_name: The name of the resource to load (key in RESOURCES dict)

    Returns:
        DataFrame containing the loaded data

    Raises:
        KeyError: If the resource_name is not found in RESOURCES

    Example:
        >>> df = load("2010_census_housing_units_by_2020_nta")
    """
    if resource_name not in RESOURCES:
        available = ", ".join(sorted(RESOURCES.keys()))
        raise KeyError(
            f"Resource '{resource_name}' not found. Available resources: {available}"
        )

    resource = RESOURCES[resource_name]
    return resource["loader"](resource["filepath"])


def list_resources() -> list[str]:
    """Return a sorted list of all available resource names."""
    return sorted(RESOURCES.keys())


def get_resource_info(resource_name: str) -> dict:
    """
    Get metadata about a resource without loading it.

    Args:
        resource_name: The name of the resource

    Returns:
        Dict containing filepath, type, data_table, and other metadata

    Raises:
        KeyError: If the resource_name is not found
    """
    if resource_name not in RESOURCES:
        raise KeyError(f"Resource '{resource_name}' not found")

    resource = RESOURCES[resource_name]
    return {
        "filepath": resource["filepath"],
        "type": resource["type"],
        "data_table": resource["data_table"],
        "sheet_name": resource.get("sheet_name", None),
    }
