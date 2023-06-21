import pandas as pd
import os
import sys
from sqlalchemy import create_engine, text
from utils import engine

template_lookup = {
    "aggregate_cdta_2020": "CDTA",
    "aggregate_block_2020": "CensusBlocks",
    "aggregate_tract_2020": "CensusTracts",
    "aggregate_councildst_2010": "CityCouncil",
    "aggregate_commntydst_2010": "CommunityDistricts",
    "aggregate_nta_2020": "NTA",
}


def read_aggregate_template(name: str):
    geo_base = pd.read_csv(
        f"data/agg_template/{template_lookup[name]}.csv",  dtype=str,
    )
    geo_base.set_index(get_index_columns(name),
                       inplace=True, verify_integrity=True)
    geo_base.drop(columns=["OBJECTID", "boro"], axis=1, inplace=True)
    return geo_base


def get_index_columns(name: str):
    index_name = {
        "aggregate_cdta_2020": "cdta2020",
        "aggregate_block_2020": "bctcb2020",
        "aggregate_tract_2020": "bct2020",
        "aggregate_councildst_2010": "councildst",
        "aggregate_commntydst_2010": "commntydst",
        "aggregate_nta_2020": "nta2020",
    }
    return index_name[name]


if __name__ == "__main__":

    table_name = sys.argv[1]

    geo_base = read_aggregate_template(table_name)
    select_query = f"SELECT * FROM {table_name}"
    with engine.begin() as conn:
        aggregate = pd.read_sql(sql=text(select_query), con=conn)

    idx = get_index_columns(table_name)
    aggregate.dropna(axis=0, subset=[idx], inplace=True)
    aggregate.set_index(idx, inplace=True, verify_integrity=True)

    df_concat = pd.concat([geo_base, aggregate], axis=1)
    final = df_concat.fillna(value=0).reset_index()

    final.to_csv(f"output/{table_name}.csv", index=False,)
