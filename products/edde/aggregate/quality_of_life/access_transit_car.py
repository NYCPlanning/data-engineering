import pandas as pd
from aggregate.clean_aggregated import order_PUMS_QOL, order_PUMS_QOL_multiple_years
from utils.PUMA_helpers import clean_PUMAs, borough_name_mapper, acs_years, year_range, sheet_name
from internal_review.set_internal_review_file import set_internal_review_files
from utils.dcp_population_excel_helpers import race_suffix_mapper, map_stat_suffix, reorder_year_race, count_suffix_mapper_global


def load_acs_access_to_car(start_year, end_year) -> pd.DataFrame:
    """Function to merge the two files for the QOL outputs and do some standard renaming. Because
    these are QOL indicators they remain in the same csv output with columns indicating year"""

    def read_excel_arg(year):
        return {
            "io": f"./resources/ACS_PUMS/EDDT_ACS{year_range(year)}.xlsx",
            "sheet_name": f"ACS{sheet_name(year)}",
            "dtype": {"Geog": str},
        }
    
    df_oldest = pd.read_excel(**read_excel_arg(start_year))
    df_latest= pd.read_excel(**read_excel_arg(end_year))

    df = pd.merge(df_oldest, df_latest, on="Geog", how="left")

    df = df.filter(regex="Geog|Wk16p|CWCar")

    df = df.replace(
        {
            "Geog": {
                "Bronx": "BX",
                "Brooklyn": "BK",
                "Manhattan": "MN",
                "Queens": "QN",
                "Staten Island": "SI",
                "NYC": "citywide",
            }
        }
    )
    df.set_index("Geog", inplace=True)

    return df


def rename_cols(df, years):
    """Rename the columns to follow conventions laid out in the wiki and issue #59"""
    cols = map(str.lower, df.columns)
    # Recode race id
    for code, race in race_suffix_mapper.items():
        cols = [col.replace(code, race) for col in cols]

    # Recode year
    year_mapper = {year[2:]: year for year in years}
    for code, year in year_mapper.items():
        cols = [col.replace(code, year) for col in cols]

    # Recode standard stat suffix
    cols = [map_stat_suffix(col, "count", True) for col in cols]

    # Rename data points
    cols = [col.replace("cwcar_", "access_carcommute_") for col in cols]
    cols = [col.replace("wk16p_", "access_workers16pl_") for col in cols]

    cols = [reorder_year_race(col) for col in cols]

    df.columns = cols
    return df


def access_transit_car(geography: str, start_year: str=acs_years[0], end_year:str=acs_years[-1], write_to_internal_review=False):
    """Main accessor for this indicator"""
    assert geography in ["puma", "borough", "citywide"]

    df = load_acs_access_to_car(start_year, end_year)

    final = rename_cols(df, [start_year, end_year])

    if geography == "citywide":
        final = df.loc[["citywide"]].reset_index().rename(columns={"Geog": "citywide"})
    elif geography == "borough":
        final = (
            df.loc[["BX", "BK", "MN", "QN", "SI"]]
            .reset_index()
            .rename(columns={"Geog": "borough"})
        )
    else:
        final = df.loc["3701":"4114"].reset_index().rename(columns={"Geog": "puma"})
        final["puma"] = final["puma"].apply(func=clean_PUMAs)

    final.set_index(geography, inplace=True)
    col_order = order_PUMS_QOL_multiple_years(
        categories=["access_carcommute", "access_workers16pl"],
        measures=count_suffix_mapper_global.values(),
        years=[start_year, end_year],
    )
    final = final.reindex(columns=col_order)
    if write_to_internal_review:
        set_internal_review_files(
            [
                (final, f"access_carcommute_{start_year}_{end_year}.csv", geography),
            ],
            "quality_of_life",
        )

    return final
