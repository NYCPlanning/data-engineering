import pandas as pd
from resources import load
from utils import geo_helpers

from aggregate.decennial_census.decennial_census_001020 import (
    load_decennial_census_001020,
)


def assault_hospitalizations(geography):
    source_data = _load_assaults()
    filtered = source_data[source_data["geo_type"] == geography]

    # For PUMA geography, use hybrid approach
    if geography == "puma":
        final = _calculate_puma_rates(filtered)
    else:
        # For borough/citywide, use pre-calculated rates
        final = pd.DataFrame(filtered["rate"]).replace({0: None})

    indicator_col_label = "safety_assaulthospital_rate"
    final.index.name = geography
    final.columns = [indicator_col_label]
    return final


def pedestrian_hospitalizations(geography):
    source_data = _load_pedestrians()
    filtered = source_data[source_data["geo_type"] == geography]

    # For PUMA geography, use hybrid approach
    if geography == "puma":
        final = _calculate_puma_rates(filtered)
    else:
        # For borough/citywide, use pre-calculated rates
        final = pd.DataFrame(filtered["rate"]).replace({0: None})

    indicator_col_label = "safety_pedhospital_rate"
    final.index.name = geography
    final.columns = [indicator_col_label]
    return final


def _calculate_puma_rates(source_data):
    """
    Hybrid approach for PUMA rates:
    - Single-CD PUMAs: use pre-calculated age-adjusted rate from source
    - Multi-CD PUMAs: aggregate counts and calculate simple rate per 100k
    """
    # Identify PUMAs with multiple CDs (duplicates in geo_id)
    puma_counts = source_data.groupby("geo_id").size()
    multi_cd_pumas = puma_counts[puma_counts > 1].index

    # For multi-CD PUMAs: aggregate counts and calculate rate
    if len(multi_cd_pumas) > 0:
        multi_cd_data = source_data[source_data.index.isin(multi_cd_pumas)]

        # Aggregate counts by PUMA first (before joining with census)
        aggregated_counts = multi_cd_data.groupby("geo_id")["count"].sum()

        # Now join with census population
        census = load_decennial_census_001020()["pop_20_count"]
        joined = aggregated_counts.to_frame().join(census)

        # Calculate rate per 100k - ensure column is named "rate"
        aggregated_rate_values = (
            (joined["count"] * 100000 / joined["pop_20_count"])
            .round(2)
            .replace({0: None})
        )
        aggregated_rates = pd.DataFrame({"rate": aggregated_rate_values})
    else:
        aggregated_rates = pd.DataFrame(columns=["rate"])

    # For single-CD PUMAs: use pre-calculated age-adjusted rate
    single_cd_data = source_data[~source_data.index.isin(multi_cd_pumas)]
    single_cd_rates = pd.DataFrame({"rate": single_cd_data["rate"]}).replace({0: None})

    # Combine both approaches
    final = pd.concat([aggregated_rates, single_cd_rates])

    return final


def _calc_geo_id(pd_row):
    match pd_row.geo_type:
        case "puma":
            # GeoID format is XYY where X=borough (1-5), YY=community district
            geo_id_str = str(pd_row.GeoID)
            borough_num = geo_id_str[0]
            comm_dist_num = int(geo_id_str[1:])
            borough_alpha = geo_helpers.borough_num_mapper[borough_num]
            return geo_helpers.community_district_to_puma(borough_alpha, comm_dist_num)
        case "borough":
            return geo_helpers.borough_name_mapper[pd_row.Geography]
        case "citywide":
            return "citywide"
        case _:
            return ""


RAW_GEO_TYPE_MAPPER = {
    "CD": "puma",  # as in, this will _will_ be a puma row after processing
    "Borough": "borough",
    "Citywide": "citywide",
}


def _load_assaults():
    df = load("assault_hospitalizations").rename(
        columns={"age_adjusted_rate_per_100k": "rate", "Number": "count"}
    )

    # Filter to the latest year only
    latest_year = df["TimePeriod"].max()
    df = df[df["TimePeriod"] == latest_year]

    df["geo_type"] = df["GeoType"].map(RAW_GEO_TYPE_MAPPER)
    df["geo_id"] = df.apply(_calc_geo_id, axis=1)

    # Clean rate column: convert suppressed values to None
    # "**" and "^^" are suppressed values in the source data
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")

    # Clean count column: remove commas and convert suppressed values to None
    df["count"] = df["count"].astype(str).str.replace(",", "")
    df["count"] = pd.to_numeric(df["count"], errors="coerce")

    return df[["geo_type", "geo_id", "rate", "count"]].set_index("geo_id")


def _load_pedestrians():
    df = load("pedestrian_hospitalizations").rename(
        columns={"rate_per_100k": "rate", "Number": "count"}
    )

    # Filter to the latest year only
    latest_year = df["TimePeriod"].max()
    df = df[df["TimePeriod"] == latest_year]

    df["geo_type"] = df["GeoType"].map(RAW_GEO_TYPE_MAPPER)
    df["geo_id"] = df.apply(_calc_geo_id, axis=1)

    # Clean rate column: convert suppressed values to None
    # "**" and "^^" are suppressed values in the source data
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")

    # Clean count column: remove commas and convert suppressed values to None
    df["count"] = df["count"].astype(str).str.replace(",", "")
    df["count"] = pd.to_numeric(df["count"], errors="coerce")

    return df[["geo_type", "geo_id", "rate", "count"]].set_index("geo_id")
