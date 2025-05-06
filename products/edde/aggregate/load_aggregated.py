"""Similar to load_data in ingest process. MAybe this is supposed to live in
external review, I'm not sure"""

# from aggregate.PUMS.count_PUMS_economics import PUMSCountEconomics
# from aggregate.PUMS.count_PUMS_households import PUMSCountHouseholds
# from aggregate.PUMS.median_PUMS_economics import PUMSMedianEconomics
from utils.PUMA_helpers import get_all_NYC_PUMAs, get_all_boroughs
import pandas as pd
from aggregate.clean_aggregated import rename_columns_demo
from utils.PUMA_helpers import sheet_name, year_range, acs_years


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


def load_clean_housing_security_pop_data(
    name_mapper: dict, start_year=acs_years[0], end_year=acs_years[-1]
) -> pd.DataFrame:
    """Function to merge the two files for the QOL outputs and do some standard renaming. Because
    these are QOL indicators they remain in the same csv output with columns indicating year
    """

    ind_name_regex = "|".join([k for k in name_mapper.keys()])

    def read_excel_arg(year):
        return {
            "io": f"./resources/ACS_PUMS/EDDE_ACS{year_range(year)}.xlsx",
            # "usecols": "A:LO",
            "dtype": {"Geog": str},
        }

    df_oldest = pd.read_excel(**read_excel_arg(start_year))
    df_latest = pd.read_excel(**read_excel_arg(end_year))

    df = pd.merge(df_oldest, df_latest, on="Geog", how="left")

    df = df.filter(regex=ind_name_regex + "|Geog")

    df.loc[df["Geog"] == "NYC", "Geog"] = "citywide"

    return df


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
