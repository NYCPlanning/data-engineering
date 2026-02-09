from pathlib import Path

import pandas as pd

COLOR_SCHEME = ["#003f5c", "#ffa600", "#58508d", "#ff6361", "#bc5090"]

BUCKET_NAME = "edm-publishing"

SQL_FILE_DIRECTORY = Path().absolute() / ".data/sql"

PAGES = {
    "Home": "home",
    "Capital Projects DB": "cpdb",
    "City Owned and Leased Properties": "colp",
    "Developments DB": "devdb",
    "Equitable Development Data Explorer": "edde",
    "Facilities DB": "facdb",
    "PLUTO": "pluto",
    "Template DB": "template",
    "Zoning Tax Lots": "ztl",
    # "Geosupport Demo": "geocode",
    "GRU QAQC": "gru",
    "Source Data": "source_data",
    "Checkbook": "checkbook",
    "Data Ingestion": "ingest",
}

DATASET_NAMES = {
    "ztl": "db-zoningtaxlots",
    "facdb": "db-facilities",
}

# NOTE this is a placeholder for an un-implemented feature
DATASET_SOURCE_VERSIONS = {
    "db-facilities": pd.DataFrame.from_records(
        [
            ("doitt_buildingfootprints", "XXX"),
            ("dcp_mappluto_wi", "XXX"),
            ("dcp_boroboundaries_wi", "XXX"),
            ("dcp_school_districts", "XXX"),
            ("dcp_policeprecincts", "XXX"),
            ("doitt_zipcodeboundaries", "XXX"),
            ("dcp_facilities_with_unmapped", "XXX"),
            ("dcp_ct2010", "XXX"),
            ("dcp_ct2020", "XXX"),
            ("dcp_councildistricts", "XXX"),
            ("dcp_cdboundaries", "XXX"),
            ("dcp_nta2010", "XXX"),
            ("dcp_nta2020", "XXX"),
        ],
        columns=["schema_name", "v"],
    )
}


def construct_dataset_by_version(dataset: str, version: str) -> str:
    return f"{dataset}_{version.replace('/', '_')}"
