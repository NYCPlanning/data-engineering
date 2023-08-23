import importlib

import pandas as pd
from pandas.core import api

from factfinder.calculate import Calculate

from . import api_key


def test_2010_aggregated_geography():
    AggregatedGeography = importlib.import_module(
        "factfinder.geography.2010"
    ).AggregatedGeography
    geography = AggregatedGeography()
    for geo in ["cd", "NTA", "cd_fp_100", "cd_park_access", "cd_fp_500"]:
        assert geo in geography.aggregated_geography


def test_2010_to_2020_aggregated_geography():
    AggregatedGeography = importlib.import_module(
        "factfinder.geography.2010_to_2020"
    ).AggregatedGeography
    geography = AggregatedGeography()
    print(geography.ratio.head())
    print(geography.lookup_geo.head())
    for geo in ["CDTA", "NTA", "CT2020"]:
        assert geo in geography.aggregated_geography


def test_2010_to_2020_ct2010_to_ct2020():
    AggregatedGeography = importlib.import_module(
        "factfinder.geography.2010_to_2020"
    ).AggregatedGeography
    geography = AggregatedGeography()
    test_data = {
        "row_1": ["test", "36005001600", 5825, 398],
        "row_2": ["test", "36005001900", 3141, 341],
    }
    test_df = pd.DataFrame.from_dict(
        test_data, orient="index", columns=["pff_variable", "census_geoid", "e", "m"]
    )
    df = geography.ct2010_to_ct2020(test_df)
    print(df)
    calculate = Calculate(api_key, 2019, "acs", "2010_to_2020")
    for var in ["pop_1", "mdage", "lgvielep1", "r550t599"]:
        df = calculate(var, "CT20")
        print(f"{var} CT2020\n", df.head())
    assert True


def test_2010_to_2020_tract_to_nta():
    calculate = Calculate(api_key, 2019, "acs", "2010_to_2020")
    for var in ["pop_1", "mdage", "lgvielep1", "r550t599"]:
        df = calculate(var, "NTA")
        print(f"{var} NTA\n", df.head())
    assert True


def test_2010_to_2020_tract_to_cdta():
    calculate = Calculate(api_key, 2019, "acs", "2010_to_2020")
    for var in ["pop_1", "mdage", "lgvielep1", "r550t599"]:
        df = calculate(var, "CDTA")
        print(f"{var} CDTA\n", df.head())
    assert True
