"""Download the pinned source files into data/sources/.

Most come from S3, the HPD Housing and Vacancy Survey public use files from nyc.gov, and DEP's MS4 map and
wastewater facility performance reports from NYC Open Data. Reads S3 credentials from the same env vars as dcpy (AWS_S3_ENDPOINT, AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY), which the repo's root .env provides through direnv.

    python scripts/download.py          # skip files already downloaded
    python scripts/download.py --force  # download everything again
"""

import argparse
import io
import os
import ssl
import urllib.request
import zipfile
from pathlib import Path

import boto3
import certifi
import duckdb
import pandas as pd
from pyogrio import raw

SOURCES_DIR = Path(__file__).parent.parent / "data" / "sources"

# local filename -> (bucket, key). Bump a version here to move the analysis to a new release.
SOURCES = {
    "pff_decennial_2010.csv": (
        "edm-publishing",
        "db-factfinder/publish/decennial/2010/2026/decennial.csv",
    ),
    "pff_decennial_2020.csv": (
        "edm-publishing",
        "db-factfinder/publish/decennial/2020/2026/decennial.csv",
    ),
    "pff_acs_2024.csv": (
        "edm-publishing",
        "db-factfinder/publish/acs/2024/2026/acs.csv",
    ),
    "kpdb_cpp_housing_growth_cd.csv": (
        "edm-publishing",
        "db-kpdb/publish/2026-10-01/cpp_housing_growth/cpp_housing_growth_cd.csv",
    ),
    "kpdb_longform_cd_output.csv": (
        "edm-publishing",
        "db-kpdb/publish/2026-10-01/aggregation/longform_cd_output.csv",
    ),
    "kpdb_cpp_housing_growth_nta.csv": (
        "edm-publishing",
        "db-kpdb/publish/2026-10-01/cpp_housing_growth/cpp_housing_growth_nta.csv",
    ),
    "kpdb_longform_nta_output.csv": (
        "edm-publishing",
        "db-kpdb/publish/2026-10-01/aggregation/longform_nta_output.csv",
    ),
    "dcp_housing.parquet": (
        "edm-recipes",
        "datasets/dcp_housing/26Q2/dcp_housing.parquet",
    ),
    "dcp_cdboundaries.parquet": (
        "edm-recipes",
        "datasets/dcp_cdboundaries/26c/dcp_cdboundaries.parquet",
    ),
    "dcp_cdta2020.parquet": (
        "edm-recipes",
        "datasets/dcp_cdta2020/26c/dcp_cdta2020.parquet",
    ),
    "dcp_ct2020.parquet": (
        "edm-recipes",
        "datasets/dcp_ct2020/26c/dcp_ct2020.parquet",
    ),
    "dcp_nta2020.parquet": (
        "edm-recipes",
        "datasets/dcp_nta2020/26c/dcp_nta2020.parquet",
    ),
    "dcp_transit_zones.parquet": (
        "edm-recipes",
        "datasets/dcp_transit_zones/20251209/dcp_transit_zones.parquet",
    ),
    "doe_school_subdistricts.parquet": (
        "edm-recipes",
        "datasets/doe_school_subdistricts/20250901/doe_school_subdistricts.parquet",
    ),
    "mta_subway_stations.parquet": (
        "edm-recipes",
        "datasets/mta_subway_stations/20261107/mta_subway_stations.parquet",
    ),
    "mta_subway_service_lines.parquet": (
        "edm-recipes",
        "datasets/mta_subway_service_lines/20251208/mta_subway_service_lines.parquet",
    ),
    "mta_subway_entrances.parquet": (
        "edm-recipes",
        "datasets/mta_subway_entrances/20251205/mta_subway_entrances.parquet",
    ),
    "mta_bus_stops.parquet": (
        "edm-recipes",
        "datasets/mta_bus_stops/20261016/mta_bus_stops.parquet",
    ),
    "mta_capital_project_locations.parquet": (
        "edm-recipes",
        "datasets/mta_capital_project_locations/20260508/mta_capital_project_locations.parquet",
    ),
    "mta_capital_project_details.parquet": (
        "edm-recipes",
        "datasets/mta_capital_project_details/20261023/mta_capital_project_details.parquet",
    ),
    "mta_capital_project_budgets.parquet": (
        "edm-recipes",
        "datasets/mta_capital_project_budgets/20261023/mta_capital_project_budgets.parquet",
    ),
    "ddc_capitalprojects_infrastructure.parquet": (
        "edm-recipes",
        "datasets/ddc_capitalprojects_infrastructure/20260701/ddc_capitalprojects_infrastructure.parquet",
    ),
    "cpdb_projects.csv": ("edm-publishing", "db-cpdb/publish/26exec/cpdb_projects.csv"),
    "cpdb_projects_by_communitydist.csv": (
        "edm-publishing",
        "db-cpdb/publish/26exec/analysis/projects_by_communitydist.csv",
    ),
    "mta_rail_stations.parquet": (
        "edm-recipes",
        "datasets/mta_rail_stations/20261107/mta_rail_stations.parquet",
    ),
    "mta_ibx_stations.parquet": (
        "edm-recipes",
        "datasets/mta_ibx_stations/20261028/mta_ibx_stations.parquet",
    ),
    "mta_ibx_service_lines.parquet": (
        "edm-recipes",
        "datasets/mta_ibx_service_lines/20261030/mta_ibx_service_lines.parquet",
    ),
    "edc_nycferry_stops.parquet": (
        "edm-recipes",
        "datasets/edc_nycferry_stops/20261108/edc_nycferry_stops.parquet",
    ),
    "edc_nycferry_routes.parquet": (
        "edm-recipes",
        "datasets/edc_nycferry_routes/20261108/edc_nycferry_routes.parquet",
    ),
    "edc_nycferry_trips.parquet": (
        "edm-recipes",
        "datasets/edc_nycferry_trips/20261108/edc_nycferry_trips.parquet",
    ),
    "edc_nycferry_stop_times.parquet": (
        "edm-recipes",
        "datasets/edc_nycferry_stop_times/20261108/edc_nycferry_stop_times.parquet",
    ),
    "dot_ferryterminals.parquet": (
        "edm-recipes",
        "datasets/dot_ferryterminals/20240805/dot_ferryterminals.parquet",
    ),
    "dep_bluebelt_bmps.parquet": (
        "edm-recipes",
        "datasets/dep_bluebelt_bmps/20240325/dep_bluebelt_bmps.parquet",
    ),
    "dcp_facilities.parquet": (
        "edm-recipes",
        "datasets/dcp_facilities/26v1/dcp_facilities.parquet",
    ),
    "dcp_cb2020.parquet": (
        "edm-recipes",
        "datasets/dcp_cb2020/26c/dcp_cb2020.parquet",
    ),
    "dpr_parksproperties.parquet": (
        "edm-recipes",
        "datasets/dpr_parksproperties/20261016/dpr_parksproperties.parquet",
    ),
    "dpr_walk_to_a_park.parquet": (
        "edm-recipes",
        "datasets/dpr_walk_to_a_park/20261028/dpr_walk_to_a_park.parquet",
    ),
    "dcp_housing_ahft.csv": (
        "edm-private",
        "datasets/dcp_housing_ahft/20261002/dcp_housing_ahft.csv",
    ),
}

# local filename -> url, for files published outside S3
HVS_URL = "https://www.nyc.gov/assets/hpd/data"
URL_SOURCES = (
    {
        f"hvs_{dataset}_{year}.csv": f"{HVS_URL}/{dataset}_puf_{year % 100}.csv"
        for dataset in ["allunits", "occupied"]
        for year in [2021, 2023]
    }
    | {
        # read by scripts/extract_dep_wastewater_map.py into seeds/, not by dbt
        "dep_wastewater_system_map.pdf": "https://www.nyc.gov/assets/dep/downloads/pdf/water/wastewater/nyc-wastewater-system-map.pdf",
        # Sea Level Rise Maps (2080s 100-year Floodplain) on NYC Open Data. The same shape as DCP GIS's
        # FutureFloodplains_2080_100yr_NPCC2014 layer in edm-private.
        "npcc_floodplain_2080_100yr.geojson": "https://data.cityofnewyork.us/api/geospatial/ek8y-fsqz?method=export&format=GeoJSON",
        # read by scripts/extract_tcol.py into seeds/, not by dbt
        "tcol_report_2026.pdf": "https://www.nyc.gov/assets/equity/downloads/pdf/2026-NYC-TCOL-Report.pdf",
    }
    | {
        # DOT's Vision Zero View layers on NYC Open Data, as GeoJSON in EPSG:4326
        f"vzv_{name}.geojson": f"https://data.cityofnewyork.us/api/geospatial/{uid}?method=export&format=GeoJSON"
        for name, uid in {
            "priority_corridors": "36nr-7fbp",
            "priority_intersections": "tmt9-43em",
            "sip_corridors": "if4c-w48d",
            "sip_intersections": "shr7-eqdc",
        }.items()
    }
)

# DEP publishes the MS4 map as a geodatabase attached to its Open Data page (j57c-rqtq) rather than as a
# queryable dataset, so this is the attachment's file URL. Its map is as of August 1, 2020.
MS4_URL = (
    "https://data.cityofnewyork.us/api/views/j57c-rqtq/files/"
    "1f13a1e9-fc70-4cbc-87db-2884b0d4378a?download=true&filename=MS4OpenData.gdb.zip"
)
# local filename -> geodatabase layer
MS4_LAYERS = {
    "ms4_drainage_areas.parquet": "MS4DRAINAGEAREAS",
    "ms4_outfalls.parquet": "MS4OUTFALLS",
    "ms4_waste_facilities.parquet": "MSWFACILITIES",
    "ms4_spdes_permits.parquet": "SPDESPERMITS",
    "ms4_bluebelt_bmps.parquet": "BLUEBELTBMP",
}


# DEP's "Wastewater Treatment Plant Performance Data" (hgue-hj96) is a set of Excel files attached to its Open Data
# page, each with one formatted report sheet per month. The main file is replaced as months are added.
WASTEWATER_PERFORMANCE_URL = "https://data.cityofnewyork.us/api/views/hgue-hj96/files"
WASTEWATER_PERFORMANCE_FILES = {
    "dep_wastewater_performance_2025.xlsx": "098480f1-11a3-436d-bfa4-fb4ab480280d",
    "dep_wastewater_performance_2024.xlsx": "80d475a5-61ad-4a6d-9433-f92a6f1b086f",
    "dep_wastewater_performance_2023.xlsx": "3ff0ddfb-d4bb-4600-a259-f1eaf1b5a79a",
    "dep_wastewater_performance_2017_2022.zip": "429825b2-d61b-4cf1-be89-864dc4d62ef4",
}
# Each sheet's report columns, by offset from its PLANT column, which sits 0 to 2 columns in depending on the
# sheet. Each concentration and removal has a column before it for a < or > qualifier. TSS is total suspended
# solids, CBOD carbonaceous biochemical oxygen demand.
WASTEWATER_PERFORMANCE_COLUMNS = {
    1: "flow_mgd",
    2: "flow_12_month_average_mgd",
    3: "flow_permit_mgd",
    4: "tss_influent_qualifier",
    5: "tss_influent_mg_l",
    6: "tss_effluent_qualifier",
    7: "tss_effluent_mg_l",
    8: "tss_effluent_permit_mg_l",
    9: "tss_removal_qualifier",
    10: "tss_removal",
    11: "tss_removal_permit",
    12: "cbod_influent_qualifier",
    13: "cbod_influent_mg_l",
    14: "cbod_effluent_qualifier",
    15: "cbod_effluent_mg_l",
    16: "cbod_effluent_permit_mg_l",
    17: "cbod_removal_qualifier",
    18: "cbod_removal",
    19: "cbod_removal_permit",
}


def fetch(url: str, path: Path) -> None:
    print(f"get     {url}")
    # uv's Python doesn't use the system's certificates, so pass certifi's
    context = ssl.create_default_context(cafile=certifi.where())
    # nyc.gov rejects urllib's default user agent
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, context=context) as response:
        path.write_bytes(response.read())


def read_wastewater_performance_sheet(sheet: pd.DataFrame) -> pd.DataFrame:
    """One row per facility from one month's report, every value kept as text."""
    header_row, plant_column = next(
        (row, column)
        for row in range(min(len(sheet), 20))
        for column in range(min(sheet.shape[1], 5))
        if str(sheet.iat[row, column]).strip() == "PLANT"
    )
    header = [
        str(sheet.iat[header_row, plant_column + offset]).strip()
        for offset in (1, 2, 3)
    ]
    assert header == ["ACTUAL", "12-M RA", "PERMIT"], header
    rows = []
    for row in range(header_row + 1, len(sheet)):
        plant = sheet.iat[row, plant_column]
        if pd.isna(plant) or not str(plant).strip():
            continue
        if str(plant).strip().upper() == "TOTAL":
            break
        values = {
            name: None
            if pd.isna(value := sheet.iat[row, plant_column + offset])
            else str(value).strip()
            for offset, name in WASTEWATER_PERFORMANCE_COLUMNS.items()
        }
        rows.append({"plant": str(plant).strip(), **values})
    return pd.DataFrame(rows)


def download_wastewater_performance(force: bool) -> None:
    """Download DEP's monthly wastewater facility reports and write every sheet's rows to one Parquet file."""
    path = SOURCES_DIR / "dep_wastewater_performance.parquet"
    for filename, file_id in WASTEWATER_PERFORMANCE_FILES.items():
        if force or not (SOURCES_DIR / filename).exists():
            fetch(
                f"{WASTEWATER_PERFORMANCE_URL}/{file_id}?download=true",
                SOURCES_DIR / filename,
            )
    if path.exists() and not force:
        print(f"exists  {path.name}")
        return
    print(f"convert {path.name}")
    workbooks = []
    for filename in WASTEWATER_PERFORMANCE_FILES:
        if filename.endswith(".zip"):
            with zipfile.ZipFile(SOURCES_DIR / filename) as archive:
                workbooks += [
                    (f"{filename}/{Path(name).name}", archive.open(name).read())
                    for name in archive.namelist()
                    if name.endswith(".xlsx")
                ]
        else:
            workbooks.append((filename, (SOURCES_DIR / filename).read_bytes()))
    sheets = []
    for source_file, content in workbooks:
        for sheet_name, sheet in pd.read_excel(
            io.BytesIO(content), sheet_name=None, header=None
        ).items():
            rows = read_wastewater_performance_sheet(sheet)
            rows.insert(0, "sheet_name", sheet_name)
            rows.insert(0, "source_file", source_file)
            sheets.append(rows)
    performance = pd.concat(sheets, ignore_index=True)  # noqa: F841 (read by DuckDB)
    duckdb.sql("SELECT * FROM performance").write_parquet(str(path))


# DEP's NYC Stormwater Flood Maps (9i7c-xyvv) are four geodatabases in one zip attached to the Open Data page.
# Only the Moderate Flood (2.13 inches per hour) with 2050 Sea Level Rise map is used.
STORMWATER_URL = (
    "https://data.cityofnewyork.us/api/views/9i7c-xyvv/files/"
    "6ce7b252-a38c-47ae-a823-680f443227e5?download=true&filename=NYCFloodStormwaterFloodMaps.zip"
)
STORMWATER_2050_GDB = (
    "NYCFloodStormwaterFloodMaps/"
    "NYC Stormwater Flood Map - Moderate Flood (2.13 inches per hr) with 2050 Sea Level Rise/"
    "NYC_Stormwater_Flood_Map_Moderate_Flood_2_13_per_hr_with_2050_Sea_Level_Rise.gdb"
)


def download_stormwater(force: bool) -> None:
    """Download DEP's stormwater flood maps and write the 2050 moderate flood layer to Parquet, geometry as WKB in
    EPSG:2263, flattened to 2D like the MS4 layers."""
    zip_path = SOURCES_DIR / "dep_stormwater_flood_maps.zip"
    path = SOURCES_DIR / "dep_stormwater_flooding_2050.parquet"
    if force or not zip_path.exists():
        fetch(STORMWATER_URL, zip_path)
    if path.exists() and not force:
        print(f"exists  {path.name}")
        return
    print(f"convert {path.name}")
    meta, _, geometry, fields = raw.read(
        f"/vsizip/{zip_path}/{STORMWATER_2050_GDB}", force_2d=True
    )
    layer_df = pd.DataFrame(dict(zip(meta["fields"], fields)))
    layer_df["geometry"] = geometry
    duckdb.sql("SELECT * FROM layer_df").write_parquet(str(path))


def download_ms4(force: bool) -> None:
    """Download DEP's MS4 geodatabase and write its layers to Parquet, geometry as WKB in EPSG:2263.

    Read through GDAL (pyogrio) and flattened to 2D, since DuckDB can't parse the layers' measured (XYZM) geometries.
    """
    zip_path = SOURCES_DIR / "ms4_open_data.gdb.zip"
    if force or not zip_path.exists():
        fetch(MS4_URL, zip_path)
    for filename, layer in MS4_LAYERS.items():
        path = SOURCES_DIR / filename
        if path.exists() and not force:
            print(f"exists  {filename}")
            continue
        print(f"convert {layer} -> {filename}")
        meta, _, geometry, fields = raw.read(
            f"/vsizip/{zip_path}/MS4OpenData.gdb", layer=layer, force_2d=True
        )
        layer_df = pd.DataFrame(dict(zip(meta["fields"], fields)))
        layer_df["geometry"] = geometry
        duckdb.sql("SELECT * FROM layer_df").write_parquet(str(path))


def main(force: bool) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    SOURCES_DIR.mkdir(parents=True, exist_ok=True)
    for filename, (bucket, key) in SOURCES.items():
        path = SOURCES_DIR / filename
        if path.exists() and not force:
            print(f"exists  {filename}")
            continue
        print(f"get     {bucket}/{key}")
        client.download_file(bucket, key, str(path))
    for filename, url in URL_SOURCES.items():
        path = SOURCES_DIR / filename
        if path.exists() and not force:
            print(f"exists  {filename}")
            continue
        fetch(url, path)
    download_ms4(force)
    download_wastewater_performance(force)
    download_stormwater(force)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    main(parser.parse_args().force)
