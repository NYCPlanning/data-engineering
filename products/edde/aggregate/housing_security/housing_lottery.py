import pandas as pd
from utils.geo_helpers import census_races, borough_name_mapper
from aggregate.load_aggregated import initialize_dataframe_geo_index


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
    data = load_lottery_data(geography, indicator)
    data = rename_columns(data, indicator)
    data = calculate_pct(data, indicator)
    data = reorder_columns(data, indicator)
    return data


def load_lottery_data(geography: str, indicator: str):
    read_csv_kwargs = {
        "filepath_or_buffer": "resources/housing_security/housing_lottery_raw.csv",
        "index_col": 0,
        "usecols": list(range(7)),
    }
    if geography == "citywide":
        read_csv_kwargs["header"] = 3
        read_csv_kwargs["nrows"] = 2
        citywide = (
            pd.read_csv(**read_csv_kwargs).replace(",", "", regex=True).astype(int)
        )
        citywide.rename(
            index={
                "applications (2014-2020)": "housing_lottery_applications",
                "signed leases (2014 - 2021)": "housing_lottery_leases",
            },
            inplace=True,
        )
        rv = citywide.loc[[indicator]]
        rv.rename({indicator: "citywide"}, inplace=True)
    if geography == "borough":
        read_csv_kwargs["nrows"] = 12
        read_csv_kwargs["header"] = 8
        borough_data = pd.read_csv(
            **read_csv_kwargs,
        )
        if indicator == "housing_lottery_applications":
            rv = borough_data.iloc[1:6, :]
        if indicator == "housing_lottery_leases":
            rv = borough_data.iloc[7:, :]
        rv = rv.replace(",", "", regex=True).astype(int, errors="ignore")
        rv.rename(index=borough_name_mapper, inplace=True)

    if geography == "puma":
        read_csv_kwargs["index_col"] = None
        read_csv_kwargs["nrows"] = 59
        if indicator == "housing_lottery_applications":
            read_csv_kwargs["header"] = 24
        elif indicator == "housing_lottery_leases":
            read_csv_kwargs["header"] = 89
        puma_data = (
            pd.read_csv(**read_csv_kwargs)
            .replace(",", "", regex=True)
            .astype(int, errors="ignore")
        )
        puma_data["Community District"] = puma_data["Community District"].astype(str)
        assert False, "fix below"
        # puma_data = community_district_to_PUMA(
        #     puma_data, "Community District", CD_abbr_type="numeric_borough"
        # )
        rv = puma_data.groupby("puma").sum(min_count=1)
    return rv


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
