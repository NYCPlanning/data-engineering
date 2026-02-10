import numpy as np
import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from utils.geo_helpers import puma_to_borough

race_coder = {
    "Asian/Pacific Islander": "_anh",
    "Black/African American": "_bnh",
    "Hispanic/Latino": "_hsp",
    "Other/Unknown": "_onh",
    "White": "_wnh",
}


def covid_death(geography: str, write_to_internal_review=False):
    """this is used to create the final dataframe and write final output to internal review files"""

    assert geography in ["citywide", "borough", "puma"]
    indicator_col_label = "health_covid19deaths"
    token_label = "_rate"

    clean_df = load_clean_source_data()

    agg_by_race = (
        clean_df.groupby([geography, "race"]).sum(numeric_only=True).reset_index()
    )
    calculate_rate_by_100k(indicator_col_label, agg_by_race)
    pivot_by_race = agg_by_race.pivot(
        index=geography, columns="race", values=indicator_col_label
    )
    pivot_by_race.replace(0, np.nan, inplace=True)  # needed for the censored datapoint

    for race in pivot_by_race.columns:
        pivot_by_race.rename(
            columns={race: f"{indicator_col_label}{race}{token_label}"}, inplace=True
        )

    agg_all_races = clean_df.groupby(geography).sum(numeric_only=True).reset_index()
    calculate_rate_by_100k(indicator_col_label, agg_all_races)
    final = pivot_by_race.merge(
        agg_all_races[[geography, indicator_col_label]],
        left_on=[geography],
        right_on=[geography],
    )
    # create the total without race breakdown
    # pivot_by_race[""] = pivot_by_race.sum(axis=1)

    # rename index and redy for output
    final = final.set_index(geography)
    final = final.sort_index(axis=1, ascending=True)
    final.index.rename(geography, inplace=True)
    final.rename(
        columns={"health_covid19deaths": "health_covid19deaths_rate"}, inplace=True
    )
    final = final.round(2)

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "total_covid_death.csv", geography)], category="quality_of_life"
        )

    return final


def calculate_rate_by_100k(new_col, aggregated):
    aggregated[new_col] = (
        aggregated["total_covid_death"] / aggregated["population"]
    ) * 10**5


def load_clean_source_data():
    source_data = pd.read_excel(
        "resources/quality_of_life/covid_death/covid_death_processed_2023.xlsx",
        sheet_name="Sheet 1",
    )
    # print(source_data)
    source_data.rename(
        columns={
            "PUMA": "puma",
            "Total\nDeaths": "total_covid_death",
            "Race/Ethnicity": "race",
        },
        inplace=True,
    )
    source_data["race"] = source_data["race"].map(race_coder)
    source_data["puma"] = "0" + source_data["puma"].astype(str)
    # create the geographies to aggregate on
    source_data["citywide"] = "citywide"
    source_data["borough"] = source_data.apply(axis=1, func=puma_to_borough)
    # handle the censored data point by replacing them with NaN
    source_data["total_covid_death"] = source_data["total_covid_death"].replace(
        "*", np.nan
    )
    source_data = add_pop_2020(source_data)
    return source_data


def add_pop_2020(df):
    census_race_coder = {
        "ANH20": "_anh",
        "BNH20": "_bnh",
        "Hsp20": "_hsp",
        "OTwoNH20": "_onh",
        "WNH20": "_wnh",
    }
    census_race_cols = list(census_race_coder.keys())
    census = pd.read_csv(
        "resources/quality_of_life/Census_Aggregations_fromErica.csv",
        header=2,
        usecols=[
            "GeogType",
            "GeoID",
        ]
        + census_race_cols,
    )

    census.columns = [census_race_coder.get(c, c) for c in census.columns]

    census = census[census["GeogType"] == "PUMA2010"]
    census["puma"] = "0" + census["GeoID"].astype(int).astype(str)
    # census = census.set_index("puma")
    census_melted = census.melt(
        id_vars=["puma"], value_vars=["_anh", "_bnh", "_hsp", "_onh", "_wnh"]
    )

    census_melted["value"] = census_melted["value"].str.replace(",", "").astype(float)
    census_melted.rename(columns={"value": "population"}, inplace=True)
    merged = df.merge(
        census_melted,
        left_on=["puma", "race"],
        right_on=["puma", "variable"],
        how="left",
    )
    return merged
