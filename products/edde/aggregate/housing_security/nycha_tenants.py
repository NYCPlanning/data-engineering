import pandas as pd
from resources import load
from utils.dcp_population_excel_helpers import race_suffix_mapper_global
from utils.geo_helpers import acs_years, clean_PUMAs, puma_to_borough

from aggregate.aggregation_helpers import order_aggregated_columns
from aggregate.decennial_census.decennial_census_001020 import decennial_census_001020

race_labels = ["", "_wnh", "_bnh", "_hsp", "_anh", "_onh"]

pop_labels = ["Total Population", "White", "Black", "Hispanic", "Asian", "Other"]


def nycha_tenants(geography: str, year: str = acs_years[-1]):
    assert geography in ["citywide", "borough", "puma"]

    clean_data = load_clean_nycha_data()
    census20 = decennial_census_001020(
        geography=geography, year=year
    )  # this is in fact pulling 2020 census but design has it mapped from 1519 to 2020

    if geography == "puma":
        final = get_percentage(pd.concat([census20, clean_data], axis=1))
    elif geography == "borough":
        agg = clean_data.groupby("borough").agg("sum")
        census20_agg = pd.concat([census20, agg], axis=1)
        final = get_percentage(census20_agg)
    elif geography == "citywide":
        agg = clean_data.groupby("citywide").agg("sum")
        census20_agg = pd.concat([census20, agg], axis=1)
        final = get_percentage(census20_agg)

    final = final.round(2)
    final.fillna(0, inplace=True)

    order_cols = order_aggregated_columns(
        df=final,
        indicators_denom=[("pop",), ("nycha_tenants",)],
        categories={
            "pop": ["pop"],
            "nycha_tenants": ["nycha_tenants"],
            "race": race_suffix_mapper_global.values(),
        },
        return_col_order=True,
        exclude_denom=True,
    )
    # 24 final columns match the results
    final_cols = [col for col in order_cols if "cv" not in col]
    final_cols = [col for col in final_cols if "moe" not in col]
    final = final.reindex(columns=final_cols)

    return final


def load_clean_nycha_data():
    nycha_data = load("nycha_tenants")[
        ["PUMA (2020)"]
        + [
            col
            for col in load("nycha_tenants").columns
            if "Public Housing" in col or "PACT" in col
        ]
    ]
    nycha_data.rename(columns={"PUMA (2020)": "puma"}, inplace=True)

    # Filter out empty/metadata rows (rows with NaN puma) and footer rows
    # Keep the "Total" row which represents citywide aggregate
    nycha_data = nycha_data[nycha_data.puma.notna() | (nycha_data.puma == "Total")]

    # Clean PUMA values - "Total" will remain as-is, PUMAs will be cleaned
    nycha_data.puma = nycha_data.puma.apply(
        func=lambda x: x if x == "Total" else clean_PUMAs(x)
    )

    # For the "Total" row, puma_to_borough would fail, so handle it separately
    # The "Total" row doesn't need a borough since it's citywide
    nycha_data["borough"] = nycha_data.apply(
        axis=1, func=lambda row: None if row.puma == "Total" else puma_to_borough(row)
    )
    nycha_data["citywide"] = "citywide"
    nycha_data.set_index("puma", inplace=True)
    # calculating the total for each race categories
    final_cols = ["borough", "citywide"]
    for pl, rl in zip(pop_labels, race_labels):
        nycha_data[f"nycha_tenants{rl}_count"] = (
            nycha_data.loc[:, "Public Housing " + pl] + nycha_data.loc[:, "PACT " + pl]
        )
        final_cols.append(f"nycha_tenants{rl}_count")

    return nycha_data[final_cols]


def get_percentage(df: pd.DataFrame):
    for r in race_labels:
        if r == "":
            df["nycha_tenants_pct"] = df["nycha_tenants_count"] / df["pop_count"] * 100
        else:
            df[f"nycha_tenants{r}_pct"] = (
                df[f"nycha_tenants{r}_count"] / df[f"pop{r}_count"] * 100
            )

    return df
