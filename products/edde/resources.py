"""
Resource manager for EDDE data sources.

This module centralizes loading of all source data files, resolving each one to a local
path via the recipe's loaded datasets (products/edde/recipe.yml -> `dcpy lifecycle builds
load`) rather than a static file under this product's directory. Requires BUILD_ENV_OUTPUT_DIR
to be set and the recipe to have been loaded first (see bash/bin/dcp_load_recipe).

Usage:
    from resources import load

    df = load("2010_census_housing_units_by_2020_nta")
"""

from pathlib import Path

import config
import pandas as pd

from dcpy.lifecycle.builds import load as build_load

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


def _load_acs_prev_year_band(path: str):
    return pd.read_excel(
        path,
        dtype={"Geog": str},
    )


def _load_acs_current_year_band(path: str):
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


# Resource registry. `dataset` is the dataset name declared in recipe.yml's
# inputs.datasets - the actual file path is resolved at load time from the loaded recipe.
RESOURCES = {
    # Housing Production
    "2010_census_housing_units_by_2020_nta": {
        "dataset": "dcp_pop_census_housing_by_puma_2010",
        "type": "csv",
        "data_table": "4.01",
        "required_columns": ["HUnits", "GeoType", "Geog"],
        "loader": _load_2010_census_housing_units,
    },
    # Decennial Census
    "decennial_census_001020": {
        "dataset": "dcp_pop_census_by_puma",
        "type": "excel",
        "data_table": "1.01",
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
    "acs_prev_year_band": {
        "dataset": "dcp_pop_acs_08_12",
        "type": "excel",
        "data_table": "1.02,1.03,1.04,2.01,2.04,2.05,2.06,3.01,3.02,3.03,3.04,3.06,3.08,5.10,5.11",
        "required_columns": ["Geog"],
        "loader": _load_acs_prev_year_band,
    },
    "acs_current_year_band": {
        "dataset": "dcp_pop_acs_20_24",
        "type": "excel",
        "data_table": "1.02,1.03,1.04,2.01,2.04,2.05,2.06,3.01,3.02,3.03,3.04,3.06,3.08,5.10,5.11",
        "required_columns": ["Geog"],
        "loader": _load_acs_current_year_band,
    },
    "census_2000": {
        "dataset": "dcp_pop_census_2000_pums",
        "type": "excel",
        "data_table": "1.02,1.03,1.04,2.01,2.04,2.05,2.06,3.01,3.02,3.03,3.04,3.06,3.08,5.10,5.11",
        "required_columns": ["GeoID"],
        "loader": _load_census_2000,
    },
    # Quality of Life - Education
    "education_outcome_data": {
        "dataset": "doe_education_math_ela_grad_by_puma",
        "type": "excel",
        "sheet_name": "Data",
        "data_table": "5.12,5.13",
        "required_columns": ["NTA Code", "NTA Name"],
        "loader": _load_education_outcome_data,
    },
    "education_outcome_data_dictionary": {
        "dataset": "doe_education_math_ela_grad_by_puma",
        "type": "excel",
        "sheet_name": "Data Dictionary",
        "data_table": "5.12,5.13",
        "required_columns": ["varlabel", "varname"],
        "loader": _load_education_outcome_data_dictionary,
    },
    # Quality of Life - Safety
    "assault_hospitalizations": {
        "dataset": "dohmh_non_fatal_assault_hospitalizations_by_puma",
        "type": "csv",
        "data_table": "5.18",
        "required_columns": [
            "Geography",
            "Number",
            "GeoType",
            "age_adjusted_rate_per_100k",
        ],
        "loader": _load_assault_hospitalizations,
    },
    "pedestrian_hospitalizations": {
        "dataset": "dohmh_pedestrian_hospitalizations_by_puma",
        "type": "csv",
        "data_table": "5.17",
        "required_columns": ["Geography", "Number", "GeoType", "rate_per_100k"],
        "loader": _load_pedestrian_hospitalizations,
    },
    # Quality of Life - Health Mortality (multi-sheet)
    "health_mortality_puma": {
        "dataset": "dohmh_death_rate_and_overdose_by_puma",
        "type": "excel",
        "sheet_name": "PUMA",
        "data_table": "5.03,5.04,5.05",
        "required_columns": ["PUMA"],
        "loader": _load_health_mortality_puma,
    },
    "health_mortality_borough": {
        "dataset": "dohmh_death_rate_and_overdose_by_puma",
        "type": "excel",
        "sheet_name": "Borough",
        "data_table": "5.03,5.04,5.05",
        "required_columns": ["Borough"],
        "loader": _load_health_mortality_borough,
    },
    "health_mortality_citywide": {
        "dataset": "dohmh_death_rate_and_overdose_by_puma",
        "type": "excel",
        "sheet_name": "City",
        "data_table": "5.03,5.04,5.05",
        "required_columns": ["City"],
        "loader": _load_health_mortality_citywide,
    },
    # Quality of Life - COVID Death
    "covid_death": {
        "dataset": "dohmh_deaths_by_race_and_puma",
        "type": "excel",
        "sheet_name": "Sheet 1",
        "data_table": "5.06",
        "required_columns": ["PUMA", "Total\nDeaths", "Race/Ethnicity"],
        "loader": _load_covid_death,
    },
    "census_aggregations": {
        "dataset": "dcp_pop_census_aggregations_by_puma",
        "type": "csv",
        "data_table": "5.06",
        "required_columns": [
            "GeogType",
            "GeoID",
            "ANH20",
            "BNH20",
            "Hsp20",
            "OTwoNH20",
            "WNH20",
        ],
        "loader": _load_census_aggregations,
    },
    # Quality of Life - Transportation Access (multi-sheet)
    "transportation_park_access": {
        "dataset": "dcp_transpo_access_by_puma",
        "type": "excel",
        "sheet_name": "Park_Qtr_Mile_Access",
        "data_table": "5.14",
        "required_columns": ["PUMA", "Pop_Served", "Total_Pop"],
        "loader": _load_transportation_park_access,
    },
    "transportation_jobs_access": {
        "dataset": "dcp_transpo_access_by_puma",
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
        "dataset": "dcp_transpo_access_by_puma",
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
        "dataset": "dcp_transpo_access_by_puma",
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
        "dataset": "nycha_occupants_by_puma",
        "type": "excel",
        "sheet_name": "Renter-occupied housing units",
        "data_table": "3.05",
        "required_columns": ["geo_id", "geo_type"],
        "loader": _load_nychvs_renter_occupied,
    },
    "nychvs_rent_stabilized": {
        "dataset": "nycha_occupants_by_puma",
        "type": "excel",
        "sheet_name": "Occupied rent stabilized",
        "data_table": "3.05",
        "required_columns": ["geo_id", "geo_type"],
        "loader": _load_nychvs_rent_stabilized,
    },
    "nychvs_occupied": {
        "dataset": "nycha_occupants_by_puma",
        "type": "excel",
        "sheet_name": "Occupied housing units",
        "data_table": "3.07",
        "required_columns": ["geo_id", "geo_type"],
        "loader": _load_nychvs_occupied,
    },
    "nychvs_three_plus_probs": {
        "dataset": "nycha_occupants_by_puma",
        "type": "excel",
        "sheet_name": "Occupied housing 3+ problems",
        "data_table": "3.07",
        "required_columns": ["geo_id", "geo_type"],
        "loader": _load_nychvs_three_plus_probs,
    },
    # Housing Security - Other
    "nycha_tenants": {
        "dataset": "nycha_public_housing_by_puma",
        "type": "excel",
        "sheet_name": "PUMA",
        "data_table": "3.11,3.12",
        "required_columns": ["PUMA (2020)", "Total Unit Count"],
        "loader": _load_nycha_tenants,
    },
    # Housing Security - HPD Housing Lottery (multi-sheet)
    "housing_lottery_applications": {
        "dataset": "hpd_housing_lottery_by_puma",
        "type": "excel",
        "sheet_name": "housing_lottery_applications",
        "data_table": "3.13",
        "required_columns": ["geog", "geo_type", "Total"],
        "loader": _load_housing_lottery_applications,
    },
    "housing_lottery_leases": {
        "dataset": "hpd_housing_lottery_by_puma",
        "type": "excel",
        "sheet_name": "housing_lottery_leases",
        "data_table": "3.14",
        "required_columns": ["geog", "geo_type", "Total"],
        "loader": _load_housing_lottery_leases,
    },
}


def _resolve_path(dataset: str) -> Path:
    """Resolve an ingested dataset name to its local path via the loaded recipe."""
    build_metadata = build_load.get_build_metadata(config.PRODUCT_PATH)
    assert build_metadata.load_result, (
        "You must run `dcp_load_recipe` (or `dcpy lifecycle builds load load`) before "
        "reading resources."
    )
    return Path(build_load.get_imported_filepath(build_metadata.load_result, dataset))


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
    path = _resolve_path(resource["dataset"])
    return resource["loader"](str(path))


def list_resources() -> list[str]:
    """Return a sorted list of all available resource names."""
    return sorted(RESOURCES.keys())


def get_resource_info(resource_name: str) -> dict:
    """
    Get metadata about a resource without loading it.

    Args:
        resource_name: The name of the resource

    Returns:
        Dict containing the source dataset name, type, data_table, and other metadata

    Raises:
        KeyError: If the resource_name is not found
    """
    if resource_name not in RESOURCES:
        raise KeyError(f"Resource '{resource_name}' not found")

    resource = RESOURCES[resource_name]
    return {
        "dataset": resource["dataset"],
        "type": resource["type"],
        "data_table": resource["data_table"],
        "sheet_name": resource.get("sheet_name", None),
    }
