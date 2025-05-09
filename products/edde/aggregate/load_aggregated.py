"""Similar to load_data in ingest process. MAybe this is supposed to live in
external review, I'm not sure"""

# from aggregate.PUMS.count_PUMS_economics import PUMSCountEconomics
# from aggregate.PUMS.count_PUMS_households import PUMSCountHouseholds
# from aggregate.PUMS.median_PUMS_economics import PUMSMedianEconomics

from collections import defaultdict
from dataclasses import dataclass
from dcpy.utils.logging import logger
import pandas as pd
import re

from aggregate.clean_aggregated import rename_columns_demo
from internal_review.set_internal_review_file import set_internal_review_files
from utils.PUMA_helpers import (
    clean_PUMAs,
    get_all_NYC_PUMAs,
    get_all_boroughs,
    borough_name_mapper,
)
from utils.PUMA_helpers import sheet_name, year_range, acs_years, dcp_pop_races
from utils.dcp_population_excel_helpers import (
    measure_suffixes,
    race_suffix_mapper_global,
    count_suffix_mapper_global,
    median_suffix_mapper_global,
)


# from aggregate.PUMS.count_PUMS_demographics import PUMSCountDemographics
# from aggregate.PUMS.median_PUMS_demographics import PUMSMedianDemographics


# categories = {
#     "demographics": [
#         ("counts", PUMSCountDemographics, False),
#         ("medians", PUMSMedianDemographics, False),
#     ],
#     "economics": [
#         ("counts", PUMSCountEconomics, False),
#         ("counts", PUMSCountHouseholds, True),
#         ("medians", PUMSMedianEconomics, False),
#     ],
# }


# def load_aggregated_PUMS(EDDT_category, geography, year, test_data):
#     """To do: include households"""
#     year_mapper = {"1519": 2019, "0812": 2012}
#     setup_directory(".output/")
#     rv = initialize_dataframe_geo_index(geography)
#     for calculation_type, aggregator_class, household in categories[EDDT_category]:
#         cache_fn = PUMS_cache_fn(
#             EDDT_category,
#             calculation_type=calculation_type,
#             year=year_mapper[year],
#             geography=geography,
#             limited_PUMA=test_data,
#             by_household=household,
#         )
#         cache_fp = f".output/{cache_fn}"
#         print(f"looking for aggregated results at {cache_fp}")
#         if path.exists(cache_fp):
#             print("found cached aggregated data")
#             data = pd.read_csv(cache_fp, dtype={geography: str})
#             data = data.set_index(geography)
#         else:
#             print(
#                 f"didn't find cached aggregated data, aggregating with {aggregator_class.__name__}"
#             )
#             aggregator = aggregator_class(limited_PUMA=test_data, geo_col=geography)
#             data = aggregator.aggregated
#             del aggregator
#         rv = rv.merge(data, left_index=True, right_index=True, how="inner")
#     return rv


def initialize_dataframe_geo_index(geography, columns=[]):
    """This should be moved to PUMA helpers and referenced in other code that merges
    to a final dataframe"""
    indicies = {
        "puma": get_all_NYC_PUMAs(),
        "borough": get_all_boroughs(),
        "citywide": ["citywide"],
    }

    rv = pd.DataFrame(index=indicies[geography], columns=columns)
    rv.index.rename(geography, inplace=True)
    return rv


"""this is specifically to use for housing security and quality March 4th POP data"""


ACS_SHORT_TO_LONG_NAMES = {
    "AfELI": "units_affordable_eli",
    "AfVLI": "units_affordable_vli",
    "AfLI": "units_affordable_li",
    "AfMI": "units_affordable_mi",
    "AfMidi": "units_affordable_midi",
    "AfHI": "units_affordable_hi",
    "GRPI30": "households_rb",
    "GRPI50": "households_erb",
    "HUPRt": "units_payingrent",
    "MdGR": "rent_median",
    "MdVl": "homevalue_median",
    "OHURt": "households_grapi",
    "OOcc1": "units_occupied_owner",
    "OcHU1": "units_occupied",
    "OcR1p": "units_overcrowded",
    "OcRU1": "units_notovercrowded",
    "ROcc": "units_occupied_renter",
}


def parse_acs_variable(raw_variable: str):
    """example: `OOcc1_B12C.1` -> {base_variable}_{maybe race_code}{year}{measure}.{maybe trailing_num}"""
    pattern = r"^(.*?)_(?:([A-Za-z]))?(\d{2})([A-Za-z])(?:\.(\d+))?$"
    matched = re.match(pattern, raw_variable)
    assert matched

    base_variable, race_code, year, measure, trailing_num = matched.groups()

    return {
        "raw_variable": raw_variable,
        "base_variable": base_variable,
        "race_code": (race_code or "").lower(),
        "year": year,
        "measure": (measure or "").lower(),
        "trailing_num": trailing_num,  # AR Note: I'm not quite sure what this is, yet.
    }


def make_acs_parsed_variables_table(acs_df: pd.DataFrame):
    """Make a metadata table about variables in the ACS.
    This will contain longform names as well as sort orders for measures and race_codes."""

    def _make_sorter(d: dict | list):
        return defaultdict(int, zip(d, range(1, len(d) + 1)))

    def _calc_dcp_indicator_name(r):
        # TODO: Add in for later pipelines: race, year
        suffix = (
            median_suffix_mapper_global[r.measure]
            if r.is_median
            else count_suffix_mapper_global[r.measure]
        )
        return f"{r.base_variable_longform}_{suffix}"

    cols = pd.DataFrame([parse_acs_variable(c) for c in acs_df.columns])

    cols["base_variable_longform"] = cols.base_variable.map(ACS_SHORT_TO_LONG_NAMES)
    cols = cols[~cols["base_variable_longform"].isnull()]

    cols["race_longform"] = cols.race_code.map(
        defaultdict(str, **race_suffix_mapper_global)
    )
    cols["measure_longform"] = cols.measure.map(defaultdict(str, **measure_suffixes))
    cols["measure_sort"] = cols.measure.map(_make_sorter(measure_suffixes))
    cols["race_sort"] = cols.race_longform.map(_make_sorter(dcp_pop_races))
    cols["is_median"] = False  # TODO
    cols["dcp_indicator_name_single_year"] = cols.apply(
        _calc_dcp_indicator_name, axis=1
    )

    cols = (
        cols.set_index(["year", "base_variable_longform"])
        .sort_values(["race_sort", "measure_sort"])
        .sort_index()
    )
    return cols


def select_acs_by_base_variable(
    acs: pd.DataFrame, year: str, dcp_base_variable: str, acs_vars_table: pd.DataFrame
):
    """Returns ACS variables for a given DCP base variable.
    Also translates the variables into the DCP variable

    e.g. The following variables exist in the ACS data for `units_affordable_eli`:
    [AfELI_23E, AfELI_23M, AfELI_23C, AfELI_23P, AfELI_23Z]

    Selcting with dcp_base_variable="units_affordable_eli" ->
    ['units_affordable_eli_count', 'units_affordable_eli_count_moe', 'units_affordable_eli_count_cv', 'units_affordable_eli_pct']

    Note: the variables are also returned in the expected order, sorted correctly for measures, race_codes, etc.
    """
    year_to = year[2:]
    col_data = acs_vars_table.loc[(year_to, dcp_base_variable)]
    acs_to_dcp_indicators = dict(
        zip(
            list(col_data.raw_variable),
            list(col_data["dcp_indicator_name_single_year"]),
        )
    )
    return acs.filter(list(acs_to_dcp_indicators.keys())).rename(
        columns=acs_to_dcp_indicators
    )


def select_acs_cols(acs_df, year, dcp_indicator_cols: list[str], acs_vars_table):
    first_col, *rest_cols = dcp_indicator_cols
    df = select_acs_by_base_variable(acs_df, year, first_col, acs_vars_table)
    for ind in rest_cols:
        df = df.join(
            select_acs_by_base_variable(acs_df, year, ind, acs_vars_table), how="left"
        )
    return df


def _calc_geog_type(geog: str):
    all_pumas = get_all_NYC_PUMAs(prefix_zeros=False)
    if geog == "citywide":
        return "citywide"
    elif geog in borough_name_mapper:
        return "borough"
    elif geog in all_pumas:
        return "puma"
    else:
        logger.error(f"Could not map geog: {geog} from the ACS")
        return ""


def load_acs(year_window: str) -> pd.DataFrame:
    """Load a year window of the ACS (e.g. 1923, meaning the ACS from 2019-2023)"""
    df = pd.read_excel(
        io=f"./resources/ACS_PUMS/EDDE_ACS{year_range(year_window)}.xlsx",
        dtype={"Geog": str},
    ).rename(columns={"Geog": "geog"})

    df.loc[df["geog"] == "NYC", "geog"] = "citywide"
    df["geog_type"] = df["geog"].map(_calc_geog_type)
    df = df.set_index("geog_type")

    df.loc["borough", "geog"] = df.loc["borough"].geog.replace(borough_name_mapper)
    df.loc["puma", "geog"] = df.loc["puma"].geog.apply(clean_PUMAs)

    return df.reset_index().set_index(["geog_type", "geog"])


def load_acs_curr_and_prev(
    start_year=acs_years[0], end_year=acs_years[-1]
) -> pd.DataFrame:
    """Load a merge together multiple year-windows of the ACS, e.g. 0812 and 1923."""
    return load_acs(start_year).merge(
        load_acs(end_year),
        left_index=True,
        right_index=True,
        how="left",
    )


@dataclass
class ACSAggregator:
    name: str
    dcp_base_variables: list[str]
    internal_review_filename: str
    internal_review_category: str

    def run(
        self, geography: str, year: str = acs_years[-1], write_to_internal_review=False
    ) -> pd.DataFrame:
        assert geography in {"citywide", "borough", "puma"}
        logger.info(f"Running {self.name} for {geography}, {year}")
        acs_df = load_acs(year)
        acs_vars_table = make_acs_parsed_variables_table(acs_df)

        acs_subset_df = select_acs_cols(
            acs_df, year, self.dcp_base_variables, acs_vars_table
        )

        final = (
            acs_subset_df.loc[geography]
            .reset_index()
            .rename(columns={"geog": geography})
            .set_index(geography)
        )

        if write_to_internal_review:
            set_internal_review_files(
                [(final, self.internal_review_filename, geography)],
                self.internal_review_category,
            )
        return final


def load_clean_pop_demographics(year: str) -> pd.DataFrame:
    """Function to merge the two files for the QOL outputs and do some standard renaming. Because
    these are QOL indicators they remain in the same csv output with columns indicating year
    """

    def read_excel_arg(year):
        return {
            "io": f"./resources/ACS_PUMS/EDDT_Dem_ACS{year_range(year)}.xlsx",
            "sheet_name": f"Dem{sheet_name(year)}",
            "usecols": "A:HR",
            "dtype": {"Geog": str},
        }

    df = pd.read_excel(**read_excel_arg(year))

    df.loc[df["Geog"] == "NYC", "Geog"] = "citywide"

    clean_data = rename_columns_demo(df, year[2:])

    clean_data.rename(columns={"geog": "Geog"}, inplace=True)

    return clean_data
