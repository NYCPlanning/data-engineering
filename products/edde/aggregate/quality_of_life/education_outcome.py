import pandas as pd

from internal_review.set_internal_review_file import set_internal_review_files
from utils import geo_helpers

SOURCE_DATA_PATH_2020 = (
    "resources/quality_of_life/education_outcome/EDDE - Math and ELA & Grad - 2024.xlsx"
)

DATA_TAB = "Data"
DATA_DICTIONARY_TAB = "Data Dictionary"


def load_edu_data() -> pd.DataFrame:
    long_to_short_col_mapper = (
        pd.read_excel(
            io=SOURCE_DATA_PATH_2020,
            sheet_name=DATA_DICTIONARY_TAB,
        )
        .set_index("varlabel")["varname"]
        .to_dict()
    )

    data = (
        pd.read_excel(
            io=SOURCE_DATA_PATH_2020,
            sheet_name=DATA_TAB,
        )
        .rename(columns=long_to_short_col_mapper)
        .set_index("ntacode")
        .join(geo_helpers.get_nta_to_puma_mapper(), how="left")
        .fillna(value=0)
        .reset_index()
        .rename(
            columns={
                "puma_code": "puma",
                "tot_grads": "tot_grad",
                "tot_grad_cohort": "tot_cohort",
            }
        )
    )

    # the totals columns are annoyingly prefixed with an extra "tot_". Strip it out.
    data.columns = [c[4:] if c.startswith("tot_") else c for c in data.columns]
    # percent total columns aren't usable, since they're aggregated at the wrong geo. Drop 'em!
    data = data.drop([c for c in data.columns if c.startswith("perct_tot_")], axis=1)

    data["borough"] = data.ntacode.str[:2]
    data["citywide"] = "citywide"
    assert data[data["puma"].isnull()].empty, "All NTAs should be mapped to a PUMA"
    return data


# TODO resolve issue with new data's racial groups
# new data seems to have replaced all caolumns for Other
# with new racial groups
RACIAL_GROUPS = [
    "ALL",
    "ASN",
    "BLK",
    "HIS",
    "OTH",
    "WHT",
]

RACE_RENAME = {
    "": "",
    "anh": "Asian",
    "bnh": "Black",
    "hsp": "Hispanic",
    "onh": "Other",
    "wnh": "White",
}
# "OTH": "onh_",

INDICATOR_MAP = {
    "ela_prof": "edu_ela",
    "math_prof": "edu_math",
    "grad": "edu_graduation",
}


def set_other_race(df: pd.DataFrame):
    # Set the Other category. It's not longer in the data
    # For example:
    # In the data there are `ela_prof_Black`, `ela_prof_White`, `ela_prof_Asian`, and `ela_prof_Hispanic`.
    # We need `ela_prof_Other` to be `ela_prof` - [all the above]

    # TODO: Verify that this is statistically correct with pop/Winnie
    all_long_race = [
        "_" + v if v else "" for v in RACE_RENAME.values() if v not in {"", "Other"}
    ]
    raw_indicators = [
        "ela_prof",
        "ela_tested",
        "math_prof",
        "math_tested",
        "grad",
        "cohort",
    ]

    for ind in raw_indicators:
        df[f"{ind}_Other"] = df[ind] - sum([df[f"{ind}{r}"] for r in all_long_race])


def calculate_edu_outcome(df: pd.DataFrame, geography: str):
    """Since there are multiple NTA codes per PUMA, we need to
    aggregate the NTAs up to the PUMA, meaning we need to recalculate all
    the percentages for each combination of indicator and ethnicity
    """

    agg = df.groupby(geography).sum().reset_index()
    set_other_race(agg)

    for short_race, long_race in RACE_RENAME.items():
        maybe_short_race = "_" + short_race if short_race else ""
        maybe_long_race = "_" + long_race if long_race else ""

        agg[f"edu_ela{maybe_short_race}_pct"] = (
            agg[f"ela_prof{maybe_long_race}"] / agg[f"ela_tested{maybe_long_race}"]
        )
        agg[f"edu_math{maybe_short_race}_pct"] = (
            agg[f"math_prof{maybe_long_race}"] / agg[f"math_tested{maybe_long_race}"]
        )
        agg[f"edu_graduation{maybe_short_race}_pct"] = (
            agg[f"grad{maybe_long_race}"] / agg[f"cohort{maybe_long_race}"]
        )

    short_races = ["_" + c if c else "" for c in RACE_RENAME]
    cols = (
        [geography]
        + [f"edu_ela{r}_pct" for r in short_races]
        + [f"edu_math{r}_pct" for r in short_races]
        + [f"edu_graduation{r}_pct" for r in short_races]
    )

    return agg[cols].set_index(geography).apply(lambda x: x * 100).round(2)


def get_education_outcome(
    geography: str, write_to_internal_review=False
) -> pd.DataFrame:
    outcomes = load_edu_data()
    result = calculate_edu_outcome(outcomes, geography)

    if write_to_internal_review:
        set_internal_review_files(
            [(result, "education_outcome.csv", geography)], category="quality_of_life"
        )

    return result
