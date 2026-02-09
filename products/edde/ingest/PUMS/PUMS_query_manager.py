"""
This class is responsible for generating list of variables and associated URLs for a
given set of variable types.

Use https://data.census.gov/mdat/#/search?ds=ACSPUMS5Y2019 as a reference.
That website provides an interface to construct a query and then see the url to
access that query via an input.

Refactor: call this from PUMS data init instead of from PUMS_request
"""

import os
from typing import List

from dotenv import load_dotenv
from ingest.PUMS.variable_generator import variables_for_url

from dcpy.utils.logging import logger

load_dotenv()


api_key = os.environ["CENSUS_API_KEY"]

NYC_PUMA_base = "7950000US360"

geo_ids = [
    (
        range(4001, 4019),  # Brooklyn
        range(3701, 3711),  # Bronx
    ),
    (
        range(4101, 4115),  # Queens
        range(3901, 3904),  # Staten Island
        range(3801, 3811),  # Manhattan
    ),
]

allowed_years = [2019, 2012]


def get_urls(
    year: int,
    variable_types: List = [],
    limited_PUMA=False,
    household=False,
    include_rw=True,
) -> dict:
    """
    :Limited_PUMA: for testing with single UCGID from each borough.
    :return:  dictionary of lists of urls. Each list is a set of of geographic regions
    associated with a group of GET variables. Each item is a set of a GET variables (either variables of interest of replicate weights)
    """
    identifiers = "SERIALNO,SPORDER,"

    geo_queries = generate_geo_queries(limited_PUMA, year)

    url_start = construct_url_start(year)
    base_weights_section = f"{url_start}?get={identifiers}"

    variable_queries = {}

    variables = variables_for_url(variable_types, year)
    variable_queries["vi"] = var_query_string(variables, household)

    if include_rw:
        for x, k in ((1, "rw_one"), (41, "rw_two")):
            if household:
                variable_queries[k] = ",".join([f"WGTP{x}" for x in range(x, x + 40)])
            else:
                variable_queries[k] = ",".join([f"PWGTP{x}" for x in range(x, x + 40)])

    urls = generate_urls(base_weights_section, geo_queries, variable_queries, year)
    return urls


def var_query_string(variables, household=False) -> str:
    if household:
        base = f"WGTP,{variables}"
    else:
        base = f"PWGTP,{variables}"

    return base


def generate_urls(base: str, geos: List, variable_queries: List, year: int):
    """Generate three urls, one for querying variables of interest
    and two for querying replicate weights

    :base: protocol, domain, path of url
    :geo: query for PUMAs and API key variable
    :variable section: maps name of url to variables in query
    :return: List of tuples. Each tuple represents a different query (for variables
    of interest or replicate weights) and each item in the tuple have geo_ids for a
    separate region
    """
    rv = {}
    for k, query in variable_queries.items():
        region_urls = []
        for geo_ids in geos:
            if year == 2019:
                region_urls.append(f"{base}{query}&ucgid={geo_ids}&key={api_key}")
            if year == 2012:
                region_urls.append(
                    f"{base}{query}&ucgid=0400000US36&PUMA00={geo_ids}&key={api_key}"
                )
                region_urls.append(
                    f"{base}{query}&ucgid=0400000US36&PUMA10={geo_ids}&key={api_key}"
                )

        rv[k] = region_urls
    return rv


def generate_geo_queries(limited_PUMA, year):
    """Geographic regions are Brooklyn, Bronx and
    Queens, Manhattan and Staten Island. These regions split the city into
    roughly two halves"""
    rv = []
    for region in geo_ids:
        all_ids = []
        for b in region:
            if year == 2019:
                geo_ids_full = [NYC_PUMA_base + str(p) for p in b]
            elif year == 2012:
                geo_ids_full = [str(p) for p in b]

            if limited_PUMA:
                all_ids.extend(geo_ids_full[0:1])
            else:
                all_ids.extend(geo_ids_full)
        rv.append(",".join(all_ids))
    return rv


def construct_url_start(year):
    if year not in allowed_years:
        logger.warning(f"{year} not one of allowed years: {allowed_years}")
        raise Exception("Unallowed year")
    base_url = f"https://api.census.gov/data/{year}/acs/acs5/pums"
    return base_url
