import pandas as pd
from resources import load
from utils import geo_helpers


def assault_hospitalizations(geography):
    source_data = _load_assaults()
    filtered = source_data[source_data["geo_type"] == geography]
    indicator_col_label = "safety_assaulthospital_rate"

    final = pd.DataFrame(filtered["rate"]).replace({0: None})
    final.index.name = geography
    final.columns = [indicator_col_label]
    return final


def pedestrian_hospitalizations(geography):
    source_data = _load_pedestrians()
    filtered = source_data[source_data["geo_type"] == geography]
    indicator_col_label = "safety_pedhospital_rate"

    final = pd.DataFrame(filtered["rate"]).replace({0: None})
    final.index.name = geography
    final.columns = [indicator_col_label]
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
        columns={"age_adjusted_rate_per_100k": "rate"}
    )

    # Filter to the latest year only
    latest_year = df["TimePeriod"].max()
    df = df[df["TimePeriod"] == latest_year]

    df["geo_type"] = df["GeoType"].map(RAW_GEO_TYPE_MAPPER)
    df["geo_id"] = df.apply(_calc_geo_id, axis=1)

    # Clean rate column: convert suppressed values to None
    # "**" and "^^" are suppressed values in the source data
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")

    return df[["geo_type", "geo_id", "rate"]].set_index("geo_id")


def _load_pedestrians():
    df = load("pedestrian_hospitalizations").rename(columns={"rate_per_100k": "rate"})

    # Filter to the latest year only
    latest_year = df["TimePeriod"].max()
    df = df[df["TimePeriod"] == latest_year]

    df["geo_type"] = df["GeoType"].map(RAW_GEO_TYPE_MAPPER)
    df["geo_id"] = df.apply(_calc_geo_id, axis=1)

    # Clean rate column: convert suppressed values to None
    # "**" and "^^" are suppressed values in the source data
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")

    return df[["geo_type", "geo_id", "rate"]].set_index("geo_id")
