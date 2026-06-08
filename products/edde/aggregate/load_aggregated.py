import re
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cache

import pandas as pd
from resources import load
from utils.dcp_population_excel_helpers import (
    count_suffix_mapper_global,
    measure_suffixes,
    median_suffix_mapper_global,
    race_suffix_mapper_global,
)
from utils.geo_helpers import (
    acs_years,
    borough_name_mapper,
    clean_PUMAs,
    dcp_pop_races,
    get_all_boroughs,
    get_all_NYC_PUMAs,
)

from aggregate.config import acs_year_suffix_map, acs_years_end_to_full
from dcpy.utils.logging import logger


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


ACS_SHORT_TO_LONG_NAMES = {
    ## Demographics
    "LEP": "lep",
    "FB": "fb",
    "PopU16": "age_popu16",
    "P16t64": "age_p16t64",
    "P65pl": "age_p65pl",
    "Pop": "pop_denom",
    "Pop5p": "age_p5pl",
    "ANH": "anh",
    "BNH": "bnh",
    "HSP": "hsp",
    "WNH": "wnh",
    ## Housing Security
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
    ## Quality of Life
    "HHlds": "access_households",
    "Comp": "access_computer",
    "BbInt": "access_broadband",
    "Wk16p": "access_workers16pl",
    "CWCar": "access_carcommute",
}
# These are named slightly differently in the final output, so we need to keep track of them
ACS_MEDIAN_VALUES_LONGFORM = {"homevalue_median", "rent_median"}


def parse_acs_variable(raw_variable: str):
    """example: `OOcc1_B12C.1` -> {base_variable}_{maybe race_code}{year}{measure}.{maybe trailing_num}"""
    pattern = r"^(.*?)_(?:([A-Za-z]))?(\d{2})([A-Za-z])(?:\.(\d+))?$"
    matched = re.match(pattern, raw_variable)
    assert matched, f"ACS column {raw_variable} does not match the variable format."

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

    def _calc_dcp_indicator_name(r, include_year=False):
        # TODO: Add in for later pipelines: race, year
        suffix = (
            median_suffix_mapper_global[r.measure]
            if r.is_median
            else count_suffix_mapper_global[r.measure]
        )
        race_var = f"_{r.race_longform}" if r.race_longform else ""
        year_var = f"_{acs_years_end_to_full[r.year]}" if include_year else ""
        return f"{r.base_variable_longform}{year_var}{race_var}_{suffix}"

    cols = pd.DataFrame([parse_acs_variable(c) for c in acs_df.columns])

    cols["base_variable_longform"] = cols.base_variable.map(ACS_SHORT_TO_LONG_NAMES)
    cols = cols[~cols["base_variable_longform"].isnull()]

    cols["race_longform"] = cols.race_code.map(
        defaultdict(str, **race_suffix_mapper_global)
    )
    cols["measure_longform"] = cols.measure.map(defaultdict(str, **measure_suffixes))
    cols["measure_sort"] = cols.measure.map(_make_sorter(measure_suffixes))
    cols["race_sort"] = cols.race_longform.map(_make_sorter(dcp_pop_races))
    cols["has_race_code"] = cols.race_longform.apply(bool)
    cols["is_median"] = cols["base_variable_longform"].isin(ACS_MEDIAN_VALUES_LONGFORM)
    cols["dcp_indicator_name_no_year"] = cols.apply(_calc_dcp_indicator_name, axis=1)
    cols["dcp_indicator_name_with_years"] = cols.apply(
        lambda r: _calc_dcp_indicator_name(r, include_year=True), axis=1
    )
    return cols


def _acs_ordered_col_mapping(
    acs_variable_table: pd.DataFrame,
    cols_longform: list[str],
    years: list[str],
    include_race: bool = True,
    include_year_in_out_col: bool = True,
    indicator_name_field: str = "",
) -> dict[str, str]:
    """calculate the ACS ordered columns to retrieve for a given list of long-form DCP columns.

    For example:
    Given a list of longform DCP column names like (like what we see in the final EDDE output)
    ["units_occupied_owner","units_occupied_renter"]

    returns:
    {'OOcc1_12E': 'units_occupied_owner_0812_count',
     'OOcc1_12M': 'units_occupied_owner_0812_count_moe',
     ...
     'OcHU1_W23P': 'units_occupied_1923_wnh_pct',
     'OcHU1_W23Z': 'units_occupied_1923_wnh_pct_moe'
    }
    where `OOcc1_12E` is a column in the ACS data.
    """
    indicator_name_field = (
        indicator_name_field or "dcp_indicator_name_with_years"
        if include_year_in_out_col
        else "dcp_indicator_name_no_year"
    )

    df = acs_variable_table.copy()
    df["variable_sort"] = df.base_variable_longform.map(
        dict(zip(cols_longform, range(1, len(cols_longform) + 1)))
    )
    df = (
        df.sort_values(
            ["year", "has_race_code", "variable_sort", "race_sort", "measure_sort"]
        )
        .loc[df["base_variable_longform"].isin(cols_longform)]
        .loc[df["year"].isin(years)]
    )
    if not include_race:
        df = df.loc[~df["has_race_code"]]

    # Filter out medians for pct and pct_moe
    df = df.loc[~(df.is_median & df.measure_longform.isin({"pct", "pct_moe"}))]

    return (
        df[["raw_variable", indicator_name_field]]
        .set_index("raw_variable")[indicator_name_field]
        .to_dict()
    )


def select_acs_cols(
    acs: pd.DataFrame,
    dcp_base_cols: list[str],
    *,
    years: list[str],
    include_race: bool = True,
    include_year_in_out_col: bool = True,
    **kwargs,
):
    """Returns ACS ... # TODO
    Also translates the variables into the DCP variable

    e.g. The following variables exist in the ACS data for `units_affordable_eli`:
    [AfELI_23E, AfELI_23M, AfELI_23C, AfELI_23P, AfELI_23Z]

    Selcting with dcp_base_variable="units_affordable_eli" ->
    ['units_affordable_eli_count', 'units_affordable_eli_count_moe', 'units_affordable_eli_count_cv', 'units_affordable_eli_pct']

    Note: the variables are also returned in the expected order, sorted correctly for measures, race_codes, etc.
    """
    acs_vars_table = make_acs_parsed_variables_table(acs)
    ordered_col_mapping = _acs_ordered_col_mapping(
        acs_vars_table,
        dcp_base_cols,
        years=years,
        include_race=include_race,
        include_year_in_out_col=include_year_in_out_col,
    )
    return acs.filter(list(ordered_col_mapping.keys())).rename(
        columns=ordered_col_mapping
    )


def _calc_geog_type(geog: str):
    if geog == "citywide":
        return "citywide"
    elif geog in borough_name_mapper:
        return "borough"
    else:
        return "puma"


def load_acs(period: str) -> pd.DataFrame:
    """
    Load an ACS period.

    Args:
        period: Either 'prev' for the previous year band (e.g., 2008-2012)
                or 'current' for the current year band (e.g., 2020-2024)
    """
    assert period in ["prev", "current"], (
        f"period must be 'prev' or 'current', got {period}"
    )

    df = load(f"acs_{period}_year_band").rename(columns={"Geog": "geog"})

    df.loc[df["geog"] == "NYC", "geog"] = "citywide"
    df["geog_type"] = df["geog"].map(_calc_geog_type)
    df = df.set_index("geog_type")

    df.loc["borough", "geog"] = df.loc["borough"].geog.replace(borough_name_mapper)
    df.loc["puma", "geog"] = df.loc["puma"].geog.apply(clean_PUMAs)

    return df.reset_index().set_index(["geog_type", "geog"])


@cache
def load_acs_curr_and_prev() -> pd.DataFrame:
    """Load and merge both the previous and current ACS year bands."""
    return load_acs("prev").merge(
        load_acs("current"),
        left_index=True,
        right_index=True,
        how="left",
    )


def load_2000_census() -> pd.DataFrame:
    df = load("census_2000").rename(columns={"GeoID": "geog"})
    df.loc[df["geog"] == "NYC", "geog"] = "citywide"
    df["geog_type"] = df["geog"].map(_calc_geog_type)
    df = df.set_index("geog_type")

    df.loc["borough", "geog"] = df.loc["borough"].geog.replace(borough_name_mapper)
    df.loc["puma", "geog"] = df.loc["puma"].geog.apply(clean_PUMAs)

    return df.reset_index().set_index(["geog_type", "geog"])


@dataclass
class ACSAggregator:
    name: str
    dcp_base_variables: list[str]
    internal_review_filename: str
    internal_review_category: str
    include_start_year: bool = True
    dcp_base_variables_conf: dict[str, bool] = field(default_factory=dict)

    def run(
        self,
        geography: str,
        start_year: str = acs_years[0],
        end_year: str = acs_years[-1],
    ) -> pd.DataFrame:
        assert geography in {"citywide", "borough", "puma"}

        # .run() is an interface that will always be called with the same periods, regardless
        # of whether we want to include the start_year.
        years = [start_year, end_year] if self.include_start_year else [end_year]
        logger.info(f"Running {self.name} for {geography}, years: {', '.join(years)}")

        # Map semantic year names to year suffixes for column selection (from config)
        year_suffixes = [acs_year_suffix_map.get(y, y) for y in years]

        # Load both ACS periods regardless of the conf. For ease/consistency
        acs_df = load_acs_curr_and_prev()

        acs_subset_df = select_acs_cols(
            acs_df,
            self.dcp_base_variables,
            years=year_suffixes,
            **self.dcp_base_variables_conf,
        )

        final = (
            acs_subset_df.loc[geography]
            .reset_index()
            .rename(columns={"geog": geography})
            .set_index(geography)
        )
        return final
