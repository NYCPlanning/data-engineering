import pandas as pd
from aggregate.aggregation_helpers import order_aggregated_columns
from internal_review.set_internal_review_file import set_internal_review_files
from utils.PUMA_helpers import borough_name_mapper, clean_PUMAs, get_all_boroughs
from aggregate.PUMS.pums_2000_economics import pums_2000_economics
from utils.PUMA_helpers import acs_years, sheet_name, year_range
from utils.dcp_population_excel_helpers import (
    race_suffix_mapper_global,
    count_suffix_mapper_global,
    median_suffix_mapper_global,
    remove_duplicate_cols,
)

occupations = ["mbsa", "srvc", "slsoff", "cstmnt", "prdtrn"]
education_levels = ["lths", "hs", "smcol", "bchpl"]
industries = [
    "agff",
    "cnstn",
    "mnfct",
    "whlsl",
    "rtl",
    "trwhu",
    "info",
    "fire",
    "pfmg",
    "edhlt",
    "arten",
    "oth",
    "pbadm",
]
ages = ["p25pl", "p16t64"]
income_bands = ["eli", "vli", "li", "mi", "midi", "hi"]

age_categories = [f"age_{a}" for a in ages]
education_categories = [f"edu_{e}" for e in education_levels]
occupation_categories = [f"occupation_{o}" for o in occupations]
industry_categories = [f"industry_{o}" for o in industries]
dcp_pop_races = ["anh", "bnh", "hsp", "wnh"]
income_band_categories = [f"households_{b}" for b in income_bands]

suffix_mappers = {
    "count": count_suffix_mapper_global,
    "median": median_suffix_mapper_global,
}


def load_clean_source_data(year: str):
    source = pd.read_excel(
        f"resources/ACS_PUMS/EDDT_HHEconSec_ACS{year_range(year)}.xlsx",
        sheet_name=f"EconSec_{sheet_name(year)}",
    )
    source["Geog"].replace(borough_name_mapper, inplace=True)
    source["Geog"].replace({"NYC": "citywide"}, inplace=True)
    source = source.set_index("Geog")

    source = remove_duplicate_cols(source)
    source = remove_duplicate_civilian_employed(source)

    source.columns = [convert_col_label(c) for c in source.columns]

    num_valid_columns = len([c for c in source.columns if "median_pct" not in c])
    col_order = order_economics(source, year)

    assert len(col_order) == num_valid_columns
    source = source.reindex(columns=col_order)
    return source


def order_economics(source_data, year):
    reorder_categories = {
        "age": age_categories,
        "education": education_categories,
        "occupation": occupation_categories,
        "industry": industry_categories,
        "misc": ["households", "cvem", "lf"],
        "race": dcp_pop_races,
    }
    indicators_denom = [
        ("age",),
        ("education",),
        ("occupation",),
        ("industry",),
        ("income band",),
        ("misc",),
    ]
    if year == "0812":
        reorder_categories["income band"] = []
    else:
        reorder_categories["income band"] = income_band_categories
    count_cols = order_aggregated_columns(
        df=None,
        indicators_denom=indicators_denom,
        categories=reorder_categories,
        return_col_order=True,
        exclude_denom=True,
    )

    assert all(col in source_data.columns for col in count_cols)
    median_cols = economics_median_cols_order()

    assert all(col in source_data.columns for col in median_cols)
    return count_cols + median_cols


def economics_median_cols_order():
    """We should have generalized median column ordering code. This needs specific function
    as not all categories are crosstabbed by race"""
    rv = []
    category_mapper = {
        "household_income": ["household_income"],
        "occupation": [f"{o}_wages" for o in occupation_categories],
        "industry": [f"{i}_wages" for i in industry_categories],
    }

    for k, i in category_mapper.items():
        categories = i
        for c in categories:
            rv.append(f"{c}_median")
            rv.append(f"{c}_median_moe")
            rv.append(f"{c}_median_cv")
        for c in categories:
            for r in dcp_pop_races:
                rv.append(f"{c}_{r}_median")
                rv.append(f"{c}_{r}_median_moe")
                rv.append(f"{c}_{r}_median_cv")

    return rv


def acs_pums_economics(
    geography, year: str = acs_years[-1], write_to_internal_review=False
):
    """Main accessor"""
    assert year in acs_years
    assert geography in ["puma", "borough", "citywide"]
    if year == "2000":
        return pums_2000_economics(
            geography, write_to_internal_review=write_to_internal_review
        )

    source = load_clean_source_data(year)

    if geography == "puma":
        final = source.loc[3701:4114]  # Don't love this but it's a common pattern
        final.index = final.index.map(clean_PUMAs)

    if geography == "borough":
        final = source.loc[get_all_boroughs()]
    if geography == "citywide":
        final = source.loc[["citywide"]]

    final.index.name = geography
    if write_to_internal_review:
        set_internal_review_files(
            [
                (final, f"ACS_PUMS_economics_{year}.csv", geography),
            ],
            "economics",
        )
    return final


def convert_col_label(col_label: str):
    indicator_label, tokens = col_label.split("_")
    wages = False
    if indicator_label[:2] == "MW":
        measure = "median"
        wages = True
        indicator_label = indicator_label[2:]
    elif indicator_label[:3] == "MdH":
        measure = "median"
    else:
        measure = "count"
    indicator_label = process_ind_label(indicator_label.lower(), wages=wages)
    if not tokens[0].isalpha():
        subgroup = ""
    else:
        subgroup = "_" + race_suffix_mapper_global[tokens[0].lower()]
        tokens = tokens[1:]
    tokens = tokens[2:]
    measure_token = suffix_mappers[measure][tokens[0].lower()]
    return f"{indicator_label}{subgroup}_{measure_token}"


def process_ind_label(indicator_label, wages=False):
    if indicator_label == "p16t64y":
        indicator_label = "p16t64"
    if indicator_label == "p25p":
        indicator_label = "p25pl"

    if indicator_label in ages:
        return f"age_{indicator_label}"

    if indicator_label in education_levels:
        return f"edu_{indicator_label}"

    if indicator_label in occupations:
        rv = f"occupation_{indicator_label}"
        if wages:
            rv = f"{rv}_wages"
        return rv

    if indicator_label in industries:
        rv = f"industry_{indicator_label}"
        if wages:
            rv = f"{rv}_wages"
        return rv

    if indicator_label in income_bands:
        return f"households_{indicator_label}"

    if indicator_label == "mdhinc":
        return "household_income"
    if indicator_label == "hhlds2":
        return "households"
    if indicator_label == "cvem1":
        return "cvem"
    return indicator_label


def remove_duplicate_civilian_employed(df: pd.DataFrame):
    """Duplicates for this column are coded by integer after indicator label
    (CvEm1_19E, CvEm2_19E)"""

    df = df.drop(df.filter(regex="CvEm[2-4]").columns, axis=1)
    df = df.drop(df.filter(regex="HHlds3").columns, axis=1)
    return df
