"""Based on Baiyue's work in
https://colab.research.google.com/gist/SPTKL/f20723e511c5aab7630c5678a3057ec3/nychvs.ipynb
which is linked in
https://github.com/NYCPlanning/db-equitable-development-tool/issues/1
"""

import pandas as pd
import requests
from ingest.HVS.HVS_geography_clean import HVS_borough_clean

metadata_url_2017 = "https://www2.census.gov/programs-surveys/nychvs/datasets/2017/microdata/stata_import_program_17.txt"
data_url_2017 = "https://www2.census.gov/programs-surveys/nychvs/datasets/2017/microdata/uf_17_occ_web_b.txt"

rw_cols_clean = [f"rw_{i}" for i in range(1, 81)]


def make_HVS_cache_fn(year: int = 2017, human_readable=True, output_type=".pkl"):
    rv = f"data/HVS_data_{year}"
    if human_readable:
        rv = f"{rv}_human_readable"

    return f"{rv}{output_type}"


def HVS_rep_weights_clean(df: pd.DataFrame) -> pd.DataFrame:
    mapper = {f"fw{i}": rw_cols_clean[i - 1] for i in range(1, 81)}
    df.rename(columns=mapper, inplace=True)
    return df


def create_HVS(year, human_readable=True, output_type=".pkl") -> pd.DataFrame:
    metadata, occupied = GET_survey_data(year)
    variable_positions = create_variable_postion_mapper(metadata)
    occupied_labels = create_label_cleaner(metadata)
    records = []
    for line in occupied.split("\n"):
        records.append(parse_line(variable_positions, line))

    HVS_data = pd.DataFrame(records)

    HVS_data = HVS_data[HVS_data["recid"] != ""]
    HVS_cache_fn = make_HVS_cache_fn(year, human_readable, output_type)
    HVS_data["boro"] = HVS_data["boro"].apply(HVS_borough_clean)

    HVS_data = clean_weights(
        HVS_data, ["fw", "chufw"] + [f"fw{x}" for x in range(1, 81)]
    )
    HVS_data = HVS_rep_weights_clean(HVS_data)
    if human_readable:
        HVS_data.rename(columns=occupied_labels, inplace=True)

    if output_type == ".pkl":
        HVS_data.to_pickle(HVS_cache_fn)
    elif output_type == ".csv":
        HVS_data.to_csv(HVS_cache_fn, index=False)
    else:
        raise Exception("Unsupported file type, data not cached nor loaded")
    return HVS_data


def clean_weights(HVS, weight_cols) -> pd.DataFrame:
    """
    https://www2.census.gov/programs-surveys/nychvs/technical-documentation/record-layouts/2017/occupied-units-17.pdf
    tells us 5 implied decimal places"""
    HVS[weight_cols] = HVS[weight_cols].astype(int) / 10**5
    return HVS


def GET_survey_data(year):
    if year == 2017:
        metadata_raw = requests.get(metadata_url_2017).text
        occupied_raw = requests.get(data_url_2017).text
        return metadata_raw, occupied_raw
    raise Exception(f"Load process for {year} HVS not implemented yet")


def create_label_cleaner(metadata_raw):
    """Use metadata to Create dict that maps default column name
    to human-readable column name."""

    occupied_labels = {}
    for label in metadata_raw.split("\n")[278:547]:
        variable = label.replace("label variable ", "").split(" ")[0]
        name = label.split('"')[1::2][0]
        occupied_labels[variable] = name
    return occupied_labels


def create_variable_postion_mapper(metadata_raw):
    """In the raw data each variable is found at characters at specific index.
    This function maps the name of each variable to the index it occurs at"""
    variable_positions = {}
    for row in metadata_raw.split("\n")[7:276]:
        parsed = row.split("\t")
        variable = parsed[1]
        mapping = parsed[2]
        if "-" in mapping:
            location_from = mapping.split("-")[0]
            location_to = mapping.split("-")[1]
        else:
            location_from = mapping
            location_to = mapping
        variable_positions[variable] = [int(location_from), int(location_to)]
    return variable_positions


def parse_line(mapping: dict, line: str) -> dict:
    parsed_line = {}
    for key, _ in mapping.items():
        parsed_line[key] = line[mapping[key][0] - 1 : mapping[key][1]].strip()
    return parsed_line
