import sys

sys.path.insert(0, "..")
import pandas as pd
import numpy as np
from _helper.geo import (
    get_hnum,
    get_sname,
    find_intersection,
    find_stretch,
    geocode,
)
from multiprocessing import Pool, cpu_count


def _import() -> pd.DataFrame:
    """
    Import _sca_capacity_projects, apply corrections, and clean inputs.

    Returns:
    df (DataFrame): Contains sca capacity project
                    data with corrections applied
                    and hnum and sname, parsed
                    from address
    """
    df = pd.read_csv("output/_sca_capacity_projects.csv")
    if len(df) == 0:
        raise ValueError("_sca_capacity_projects has no rows!")

    def _apply_corrections(
        data: pd.DataFrame, corrections: pd.DataFrame, field: str
    ) -> pd.DataFrame:
        print(f"Correcting field '{field}'")
        data = data.copy()
        cor_add_dict = corrections.to_dict("records")
        for record in cor_add_dict:
            # use school name and borough to find record to correct
            old_values = data.loc[
                (data["name"] == record["school"])
                & (data["borough"] == record["borough"])
            ][field].to_list()
            if not old_values:
                raise KeyError(
                    f"No school found with name and borough ('{record['school']}', '{record['borough']}')"
                )
            if len(old_values) > 1:
                raise ValueError(
                    f"Multiple '{field}' values found for school '{record['school']}': {old_values}"
                )
            old_value = old_values[0]
            print(
                f"""Correcting '{field}' for school ('{record["school"]}', ''{record["borough"]}')
                from {old_value}
                to {record[field].upper()}"""
            )
            data.loc[
                (data["name"] == record["school"])
                & (data["borough"] == record["borough"]),
                field,
            ] = record[field].upper()
        return data

    # Import csv to replace invalid addresses with manual corrections
    cor_add = pd.read_csv(
        "../_data/sca_capacity_address_cor.csv", dtype=str, engine="c"
    )
    df = _apply_corrections(df, cor_add, "address")

    # Import csv to replace org_levels with manual corrections
    cor_org = pd.read_csv(
        "../_data/sca_capacity_org_level_cor.csv", dtype=str, engine="c"
    )
    df = _apply_corrections(df, cor_org, "org_level")

    # Clean address column
    df["address"] = df["address"].replace(np.nan, "")

    # Parse stretches
    df[["streetname_1", "streetname_2", "streetname_3"]] = df.apply(
        lambda row: pd.Series(find_stretch(row["address"])), axis=1
    )

    # Parse intersections
    df[["streetname_1", "streetname_2"]] = df.apply(
        lambda row: pd.Series(find_intersection(row["address"])), axis=1
    )

    """
    # Clean inputs for geocoding
    df['hnum'] = df.address.apply(get_hnum).apply(lambda x: clean_house(x))
    df['sname'] = df.address.apply(get_sname).apply(lambda x: clean_street(x))
    """

    # Parse house numbers
    df["hnum"] = (
        df["address"]
        .astype(str)
        .apply(get_hnum)
        .apply(lambda x: x.split("/", maxsplit=1)[0] if x is not None else x)
    )

    # Parse street names
    df["sname"] = df["address"].astype(str).apply(get_sname)

    return df


def _geocode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Geocode cleaned sca data

    Parameters:
    df (DataFrame): Contains sca capacity project
                    data with corrections applied
                    and hnum and sname, parsed
                    from address
    Returns:
    df (DataFrame): Contains input fields along
                    with geosupport fields
    """
    records = df.to_dict("records")
    del df

    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)

    df = pd.DataFrame(it)
    df["geo_longitude"] = pd.to_numeric(df["geo_longitude"], errors="coerce")
    df["geo_latitude"] = pd.to_numeric(df["geo_latitude"], errors="coerce")

    return df


def _output(df):
    """
    Output geocoded data.

    CSV contains all records. All except special
    education schools get output to stdout for transfer
    to postgres.

    Parameters:
    df (DataFrame): Contains input fields along
                    with geosupport fields
    """
    cols = [
        "uid",
        "name",
        "org_level",
        "district",
        "capacity",
        "pct_ps",
        "pct_is",
        "pct_hs",
        "guessed_pct",
        "start_date",
        "capital_plan",
        "borough",
        "address",
        "geo_xy_coord",
        "geo_x_coord",
        "geo_y_coord",
        "geo_from_x_coord",
        "geo_from_y_coord",
        "geo_to_x_coord",
        "geo_to_y_coord",
        "geo_function",
        "geo_grc",
        "geo_grc2",
        "geo_reason_code",
        "geo_message",
    ]
    df[cols].to_csv("output/all_capacity_projects.csv")

    # Remove special ed cases
    df_filtered = df[
        (df["district"] != "75") & (df.org_level != "PK") & (df.org_level != "3K")
    ]

    df_filtered[cols].to_csv("output/sca_capacity_projects.csv", index=False)


if __name__ == "__main__":
    df = _import()
    df = _geocode(df)
    _output(df)
