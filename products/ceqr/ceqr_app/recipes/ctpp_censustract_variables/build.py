import math
import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

County = {
    "36061": "New York",
    "36047": "Kings",
    "36005": "Bronx",
    "36081": "Queens",
    "36085": "Richmond",
}

MODE = {
    "1": "trans_total",
    "2": "trans_auto_solo",
    "3": "trans_auto_2",
    "4": "trans_auto_3",
    "5": "trans_auto_4",
    "6": "trans_auto_5_or_6",
    "7": "trans_auto_7_or_more",
    "8": "trans_public_bus",
    "9": "trans_public_streetcar",
    "10": "trans_public_subway",
    "11": "trans_public_rail",
    "12": "trans_public_ferry",
    "13": "trans_bicycle",
    "14": "trans_walk",
    "15": "trans_taxi",
    "16": "trans_motorcycle",
    "17": "trans_other",
    "18": "trans_home",
}

nyc = ["New York", "Kings", "Queens", "Bronx", "Richmond"]


def compute_value(total_value):
    """
    This function is used for calculating
    the estimate value for trans_auto_carpool_total,
    trans_auto_total and trans_public_total
    """
    total_value["trans_auto_carpool_total"] = (
        total_value.trans_auto_2
        + total_value.trans_auto_3
        + total_value.trans_auto_4
        + total_value.trans_auto_5_or_6
        + total_value.trans_auto_7_or_more
    )

    total_value["trans_auto_total"] = (
        total_value.trans_auto_solo
        + total_value.trans_auto_2
        + total_value.trans_auto_3
        + total_value.trans_auto_4
        + total_value.trans_auto_5_or_6
        + total_value.trans_auto_7_or_more
    )

    total_value["trans_public_total"] = (
        total_value.trans_public_bus
        + total_value.trans_public_streetcar
        + total_value.trans_public_subway
        + total_value.trans_public_rail
        + total_value.trans_public_ferry
    )
    total_value = total_value.melt(
        "geoid", var_name="variable", value_name="value"
    ).sort_values(by=["geoid", "variable"])

    total_value["value"] = total_value.value.astype(int)

    return total_value


def compute_moe(total_moe):
    """
    This function is used for calculating
    the moe value for trans_auto_carpool_total,
    trans_auto_total and trans_public_total
    """
    total_moe["trans_auto_carpool_total"] = (
        total_moe.trans_auto_2**2
        + total_moe.trans_auto_3**2
        + total_moe.trans_auto_4**2
        + total_moe.trans_auto_5_or_6**2
        + total_moe.trans_auto_7_or_more**2
    )

    total_moe["trans_auto_total"] = (
        total_moe.trans_auto_solo**2
        + total_moe.trans_auto_2**2
        + total_moe.trans_auto_3**2
        + total_moe.trans_auto_4**2
        + total_moe.trans_auto_5_or_6**2
        + total_moe.trans_auto_7_or_more**2
    )

    total_moe["trans_public_total"] = (
        total_moe.trans_public_bus**2
        + total_moe.trans_public_streetcar**2
        + total_moe.trans_public_subway**2
        + total_moe.trans_public_rail**2
        + total_moe.trans_public_ferry**2
    )

    total_moe["trans_auto_carpool_total"] = total_moe.trans_auto_carpool_total.apply(
        lambda x: math.sqrt(x)
    )
    total_moe["trans_auto_total"] = total_moe.trans_auto_carpool_total.apply(
        lambda x: math.sqrt(x)
    )
    total_moe["trans_public_total"] = total_moe.trans_auto_carpool_total.apply(
        lambda x: math.sqrt(x)
    )

    total_moe = total_moe.melt(
        "geoid", var_name="variable", value_name="moe"
    ).sort_values(by=["geoid", "variable"])

    total_moe["moe"] = total_moe.moe.astype(int)

    return total_moe


def etl(data):
    """
    This function is used for transforming
    the ctpp data to the required output
    schema
    """
    # parse the geoid to extract the standard part
    data["geoid"] = data.geoid.apply(lambda x: x.split("US")[1][-12:])

    # convert value and moe to integer and float
    data["value"] = data.value.astype(int)
    data["moe"] = data.moe.astype(float)

    # filter the data to keep only NYC records
    data["county"] = data.geoid.apply(lambda x: County.get(x[:-6], ""))
    data = data[data.county.isin(nyc)].iloc[:, :-1]

    # convert variable to its real definition
    data["variable"] = data.variable.apply(lambda x: MODE.get(str(x), ""))

    # calculate value for trans_auto_carpool_total, trans_auto_total and trans_public_total
    total_value = data.pivot("geoid", "variable", "value").reset_index()
    total_value = total_value.fillna(0)
    total_value = compute_value(total_value)

    # calculate moe for trans_auto_carpool_total, trans_auto_total and trans_public_total
    total_moe = data.pivot("geoid", "variable", "moe").reset_index()
    total_moe = total_moe.fillna(0)
    total_moe = compute_moe(total_moe)

    # merge value table with the moe table
    data = pd.merge(total_value, total_moe, on=["geoid", "variable"])
    data["moe"] = data.moe.astype(int)
    data = data[["geoid", "value", "moe", "variable"]]

    return data


def add_zero(num):
    try:
        if num[0] == ".":
            return "0" + num
        else:
            return num
    except:  # noqa: E722
        return num


if __name__ == "__main__":
    df = pd.read_sql(
        """
            SELECT 
                geoid, 
                lineno AS variable, 
                est AS value, moe 
            FROM ctpp_mode_split_ny."2012_2016"
        """,
        con=create_engine(os.environ["RECIPE_ENGINE"]),
    )

    # clean value and moe
    df["value"] = df["value"].apply(lambda x: x.replace(",", "")).astype(int)
    df["moe"] = (
        df["moe"]
        .apply(lambda x: x.replace(",", "").replace("+/-", ""))
        .replace(r"^\s*$", np.nan, regex=True)
        .apply(add_zero)
        .astype(float)
    )

    # conduct data etl
    df = etl(df)
    df.to_csv(sys.stdout, columns=["geoid", "value", "moe", "variable"], index=False)
