import sys

import pandas as pd

sys.path.insert(0, "..")
from _helper.geo import get_borocode


def _import() -> pd.DataFrame:
    """
    Download and format nysdec air monitoring station data from open data API

    Gets raw data from API and saves to output/raw.csv
    Checks raw data to ensure necessary columns are included
    Gets borocode from county name

    Returns:
    df (DataFrame): Contains fields region, site_id, monitor_type,
            county, site_name, air_quality_system_id, latitude,
            longitude, ozone, so2, nox, co, pm-2.5, cpm-2.5,
            pm-10, cpm-10, lead, specification, continuous_speciation,
            metals, toxics, carbonyls, acid_rain, pams, mercury, location,
            borocode
    """
    url = "https://data.ny.gov/api/views/qcpj-zdb6/rows.csv"
    df = pd.read_csv(url, dtype=str, engine="c", index_col=False)
    df.to_csv("output/raw.csv", index=False)

    df.columns = [i.lower().replace(" ", "_") for i in df.columns]
    cols = [
        "region",
        "site_id",
        "monitor_type",
        "county",
        "site_name",
        "air_quality_system_id",
        "latitude",
        "longitude",
        "ozone",
        "so2",
        "nox",
        "co",
        "pm-2.5",
        "cpm-2.5",
        "pm-10",
        "cpm-10",
        "lead",
        "speciation",
        "continuous_speciation",
        "metals",
        "toxics",
        "carbonyls",
        "acid_rain",
        "pams",
        "mercury",
        "location",
    ]
    for col in cols:
        assert col in df.columns

    df["borocode"] = df.county.apply(get_borocode)
    df = df.loc[df.region == "2", :]
    return df


def _output(df):
    """
    Output data to stdout for transfer to postgres
    """
    cols = [
        "site_id",
        "monitor_type",
        "borocode",
        "county",
        "site_name",
        "air_quality_system_id",
        "latitude",
        "longitude",
        "ozone",
        "so2",
        "nox",
        "co",
        "pm-2.5",
        "cpm-2.5",
        "pm-10",
        "cpm-10",
        "lead",
        "speciation",
        "continuous_speciation",
        "metals",
        "toxics",
        "carbonyls",
        "acid_rain",
        "pams",
        "mercury",
        "location",
    ]
    df[cols].to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    df = _import()
    _output(df)
