import pandas as pd

from utils.geo_helpers import (
    census_races,
    clean_PUMAs,
    borough_name_mapper,
    get_all_boroughs,
    get_all_NYC_PUMAs,
)


demographic_indicators_denom = [
    ("total_pop",),
    ("age_p5pl",),
    ("age_bucket",),
    ("LEP", "over_five_filter"),
    ("foreign_born",),
]


def order_aggregated_columns(
    df: pd.DataFrame,
    indicators_denom,
    categories,
    household=False,
    exclude_denom=False,
    demographics_category=False,
    return_col_order=False,
) -> pd.DataFrame:
    """This can be DRY'd out, written quickly to meet deadline"""
    col_order = []
    for ind_denom in indicators_denom:
        ind = ind_denom[0]
        for ind_category in categories[ind]:
            for measure in ["_count", "_pct"]:
                col_order.append(f"{ind_category}{measure}")
                col_order.append(f"{ind_category}{measure}_moe")
                if measure == "_count":
                    col_order.append(f"{ind_category}{measure}_cv")
            if not exclude_denom:
                print("adding denom")
                col_order.append(f"{ind_category}_pct_denom")
            # if exclude_denom and ind == "LEP":
            #    col_order.append("age_p5pl")
        if not household:
            for ind_category in categories[ind]:
                for race_crosstab in categories["race"]:
                    for measure in ["_count", "_pct"]:
                        column_label_base = f"{ind_category}_{race_crosstab}{measure}"
                        col_order.append(f"{column_label_base}")
                        col_order.append(f"{column_label_base}_moe")
                        if measure == "_count":
                            col_order.append(f"{column_label_base}_cv")
                    if not exclude_denom:
                        col_order.append(f"{ind_category}_{race_crosstab}_pct_denom")
                    # if exclude_denom and ind == "LEP":
                    #    col_order.append(f"age_p5pl_{race_crosstab}")

    if exclude_denom and demographics_category:
        col_order.extend(median_age_col_order(categories["race"]))
    if return_col_order:
        return col_order
    return df.reindex(columns=col_order)


def median_age_col_order(race_crosstabs):
    """Order median age columns. The calculate_median_LI.py code does this ordering
    automatically but data coming from others sources needs to be ordered this way"""
    col_order = []
    for crosstab in [""] + [f"_{r}" for r in race_crosstabs]:
        for measure in ["", "_moe", "_cv"]:
            col_order.append(f"age_median{crosstab}_median{measure}")
    return col_order


def get_category(indicator, data=None):
    """Outdated now that we use dcp_pop_races for race crosstabs"""
    if indicator == "age_bucket":
        return ["age_popu16", "age_p16t64", "age_p65pl"]
    elif indicator == "household_income_bands":
        return [
            "ELI",
            "VLI",
            "LI",
            "MI",
            "MIDI",
            "HI",
        ]
    elif indicator == "education":
        return [
            "Bachelors_or_higher",
            "Some_college",
            "high_school_or_equiv",
            "less_than_hs_or_equiv",
        ]
    elif indicator == "race":
        return census_races
    else:
        categories = list(data[indicator].unique())
        if None in categories:
            categories.remove(None)
        categories.sort()
        return categories


def get_geography_pop_data(clean_data: pd.DataFrame, geography: str):
    if geography == "citywide":
        final = (
            clean_data.loc[clean_data["Geog"] == "citywide"]
            .rename(columns={"Geog": "citywide"})
            .copy()
        )
    elif geography == "borough":
        boros = get_all_boroughs()
        clean_data["Geog"] = clean_data["Geog"].map(
            borough_name_mapper, na_action="ignore"
        )
        final = (
            clean_data.loc[clean_data["Geog"].isin(boros)]
            .rename(columns={"Geog": "borough"})
            .copy()
        )
    elif geography == "puma":
        pumas = get_all_NYC_PUMAs()
        clean_data["Geog"] = clean_data["Geog"].apply(func=clean_PUMAs)
        final = (
            clean_data.loc[clean_data["Geog"].isin(pumas)]
            .rename(columns={"Geog": "puma"})
            .copy()
        )
    final.set_index(geography, inplace=True)

    return final
