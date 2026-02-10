import pandas as pd
from aggregate.load_aggregated import initialize_dataframe_geo_index
from utils.geo_helpers import (
    borough_name_mapper,
    borough_num_mapper,
    census_races,
    community_district_to_puma,
)


def _calc_borough(r):
    if r.geo_type == "borough":
        return borough_name_mapper[r.raw_geog]
    elif r.geo_type == "cd":
        boro_num = str(r.raw_geog)[0:1]
        return borough_num_mapper[boro_num]
    else:
        return ""


def _geo_for_type(r):
    if r.geo_type == "citywide":
        return "citywide"
    elif r.geo_type == "borough":
        return r.borough
    else:
        return r.puma


def _load_lottery_xlsx(sheet_name):
    df = pd.read_excel(
        "resources/housing_security/hpd_housing_lottery_2025.xlsx",
        dtype={"geog": str},
        sheet_name=sheet_name,
    ).rename(columns={"geog": "raw_geog"})
    df["citywide"] = "citywide"
    df["borough"] = df.apply(_calc_borough, axis=1)
    df.loc[df["geo_type"] == "cd", "geo_type"] = "puma"
    df.loc[df["geo_type"] == "puma", "puma"] = df[df["geo_type"] == "puma"].apply(
        lambda r: community_district_to_puma(r.borough, str(r.raw_geog)[1:]), axis=1
    )
    df["geo"] = df.apply(_geo_for_type, axis=1)
    return df.set_index(["geo_type", "geo"])


def housing_lottery_applications(geography) -> pd.DataFrame:
    final = initialize_dataframe_geo_index(geography)
    applications = lottery_data(geography, "housing_lottery_applications")
    final = final.merge(applications, left_index=True, right_index=True)
    return final


def housing_lottery_leases(geography) -> pd.DataFrame:
    final = initialize_dataframe_geo_index(geography)
    leases = lottery_data(geography, "housing_lottery_leases")
    final = final.merge(leases, left_index=True, right_index=True)
    return final


def lottery_data(geography: str, indicator: str):
    assert indicator in ["housing_lottery_applications", "housing_lottery_leases"]
    data = _load_lottery_xlsx(indicator).loc[geography].rename_axis(geography)

    # In the case when PUMAs are inferred from Community Districts, there will be
    # potentially duplicated PUMAS, e.g. both  Community Districts 1 & 2 will map
    # to 04121, so we need to dedupe
    data = data.groupby(level=0).sum()

    data = rename_columns(data, indicator)
    data = calculate_pct(data, indicator)
    data = reorder_columns(data, indicator)
    return data


def rename_columns(df: pd.DataFrame, indicator: str) -> pd.DataFrame:
    return df.rename(
        columns={
            "Total": f"{indicator}_count",
            "Asian NH": f"{indicator}_anh_count",
            "Black NH": f"{indicator}_bnh_count",
            "Hispanic": f"{indicator}_hsp_count",
            "White NH": f"{indicator}_wnh_count",
            "All other": f"{indicator}_onh_count",
        }
    )


def calculate_pct(df: pd.DataFrame, indicator: str) -> pd.DataFrame:
    for r in census_races:
        df[f"{indicator}_{r}_pct"] = (
            df[f"{indicator}_{r}_count"] / df[f"{indicator}_count"]
        ) * 100
    return df


def reorder_columns(df, indicator):
    indicator_order = [f"{indicator}_count"]
    for r in census_races:
        for measure in ["count", "pct"]:
            indicator_order.append(f"{indicator}_{r}_{measure}")
    return df.reindex(columns=indicator_order)
