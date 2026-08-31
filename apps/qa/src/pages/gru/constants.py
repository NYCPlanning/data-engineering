from dataclasses import dataclass

import pandas as pd

bucket = "edm-publishing"

CHECKS_REPO = "db-gru-qaqc"
CHECKS_WORKFLOW = "main.yml"

INGEST_REPO = "data-engineering"
INGEST_WORKFLOW = "ingest_single.yml"
INGEST_WORKFLOW_URL = (
    f"https://github.com/NYCPlanning/{INGEST_REPO}/actions/workflows/{INGEST_WORKFLOW}"
)

qa_checks = pd.DataFrame(
    [
        (
            "Address Points vs PAD",
            "address-points-vs-pad",
            ["dcp_addresspoints"],
        ),
        (
            "Address Points (Spatial) vs GRID",
            "addresses-spatial",
            ["dcp_atomicpolygons", "dcp_addresspoints"],
        ),
        (
            "Footprint BINs vs PAD",
            "footprints-vs-pad",
            ["doitt_buildingfootprints"],
        ),
        (
            "Historical Footprint BINs vs PAD",
            "historical-footprints-vs-pad",
            ["doitt_buildingfootprints_historical", "doitt_buildingfootprints"],
        ),
        ("TBINs vs. C/Os", "housing", ["dcp_developments"]),
        (
            "PAD BINs vs Footprint BINs",
            "pad-vs-footprint",
            ["doitt_buildingfootprints", "dcp_pad"],
        ),
        (
            "DCM Names vs SND Names",
            "dcm-streetname",
            ["dcp_dcmstreetcenterline"],
        ),
        (
            "Generic SAF Addresses vs PAD Roadbed SAF Addresses vs PAD",
            "saf-vs-pad",
            ["dcp_saf"],
        ),
    ],
    columns=["display_name", "action_name", "sources"],
)


@dataclass(frozen=True)
class SourceDataset:
    """A source dataset the checks read, and where its newest version comes from."""

    id: str
    refresh: str
    """How a new version reaches edm-recipes, in a reviewer's terms."""

    upstream_kind: str | None = None
    """Which connector holds the newest version at the origin: "template" (ask the ingest
    template's own source connector), "bytes", or "publishing".

    None means there is no archive step that can fall behind, so there is nothing to compare
    against. Only dcp_saf, which the checks read straight out of edm-publishing.
    """

    upstream_key: str | None = None
    """Key for that connector, where it differs from the dataset id."""


BYTES_QUARTERLY = "Bytes quarterly release. Archived by ingest_bytes_quarterly.yml, dispatched by hand."
GIS_UPLOAD = (
    "GIS drops a new version to edm-publishing/datasets. Archiving it is manual."
)
OPEN_DATA_WEEKLY = (
    "Open Data. Archived automatically by ingest_open_data.yml every Sunday."
)

source_datasets = {
    source.id: source
    for source in [
        SourceDataset(
            id="dcp_addresspoints",
            upstream_kind="template",
            refresh=GIS_UPLOAD,
        ),
        SourceDataset(
            id="dcp_atomicpolygons",
            upstream_kind="bytes",
            upstream_key="lion.atomic_polygons",
            refresh=BYTES_QUARTERLY,
        ),
        SourceDataset(
            id="dcp_dcmstreetcenterline",
            upstream_kind="template",
            refresh=GIS_UPLOAD,
        ),
        SourceDataset(
            id="dcp_developments",
            upstream_kind="publishing",
            upstream_key="db-developments",
            refresh=(
                "Published by a DevDB build. Archiving it is manual: the workflow that used "
                "to do it, developments_publish.yml, is in .github/workflows/archive."
            ),
        ),
        SourceDataset(
            id="dcp_pad",
            upstream_kind="bytes",
            upstream_key="lion.property_address_directory",
            refresh=BYTES_QUARTERLY,
        ),
        SourceDataset(
            id="dcp_saf",
            refresh=(
                "GIS uploads to edm-publishing/gru/dcp_saf. The checks read it from there, so "
                "it is never archived to edm-recipes and cannot fall behind a newer version."
            ),
        ),
        SourceDataset(
            id="doitt_buildingfootprints",
            upstream_kind="template",
            refresh=OPEN_DATA_WEEKLY,
        ),
        SourceDataset(
            id="doitt_buildingfootprints_historical",
            upstream_kind="template",
            refresh=OPEN_DATA_WEEKLY,
        ),
    ]
}

readme_markdown_text = """### Source data

The checks read source data from `edm-recipes`, archived there by ingest in the
[data-engineering](https://github.com/NYCPlanning/data-engineering) repo. This repo only runs the
checks themselves. The table above compares what is archived against the newest version at each
origin, so a stale source shows up before a check runs on it rather than after.

Three of the eight need a person to archive them:

+ **`dcp_addresspoints`** and **`dcp_dcmstreetcenterline`** land in `edm-publishing/datasets` when
  the GIS team drops them. Nothing archives them on a schedule.
+ **`dcp_developments`** comes from a DevDB build published to `edm-publishing`. The workflow that
  used to archive it on publish now sits in `.github/workflows/archive`.

The rest are on a schedule:

+ **`dcp_atomicpolygons`** and **`dcp_pad`** come from Bytes of the Big Apple, once a quarter.
  [`ingest_bytes_quarterly.yml`](https://github.com/NYCPlanning/data-engineering/actions/workflows/ingest_bytes_quarterly.yml)
  archives both, along with LION and the district boundary files, but someone has to dispatch it
  with the new quarter.
+ **`doitt_buildingfootprints`** and **`doitt_buildingfootprints_historical`** come from Open Data
  and are archived every Sunday by
  [`ingest_open_data.yml`](https://github.com/NYCPlanning/data-engineering/actions/workflows/ingest_open_data.yml).
  Nothing to run by hand.
+ **`dcp_saf`** is a GIS upload to `edm-publishing/gru/dcp_saf`. The checks read it straight from
  there, so it is never archived and never behind.

The Ingest buttons above dispatch
[`ingest_single.yml`](https://github.com/NYCPlanning/data-engineering/actions/workflows/ingest_single.yml)
on `main` for one dataset at the version shown, so an out of date source can be refreshed without
leaving this page.

### PAD checks

#### Check that CSCL-derived address points exist in PAD

The output of this check contains records that were not successfully geocoded with
geosupport function 1A, as well as those that only matched a pseudo-address.

#### Identify which CSCL-derived address points existing in PAD don't match PAD BIN

The output of this check contains address point records that were successfully geocoded with
geosupport function 1A but address point BIN doesn't match with geosupport BIN.

#### Identify address points that match to different atomicids in PAD and Geosupport

The output of this check contains atomic polygon mismatches between results from spatial join and the ones returned by Geosupport function 1E.

For address points that didn't get hit by Geosupport function 1E, they can be found in `rejects_address_spatial` table in the output folder.

#### Check that addresses in PAD have an associated DOITT bulding footprint

This check merges PAD addresses on DOITT building footprints using BIN. Records in PAD that do not succesfull match with a building footprint are output for QAQC.

#### Check that SAF addresses exist in PAD

The output of this check contains SAF records that were not successfully geocoded with
geosupport function 1, 1A, or 1R. SAF records come from the following files:

+ GenericABCEGNPX
+ GenericD
+ GenericOV
+ GenericS
+ RoadbedABCEGNPX
+ RoadbedD
+ RoadbedOV
+ RoadbedS

Results are organized into 6 files -- three for generic and three for roadbed.
Within these six, two geocode using 1A, two use 1, and two use 1 with the roadbed switch.

### TPAD checks

#### Make sure DOITT bulding footprint BINs are not in TPAD

The output of this check contains records that matched a TPAD record when geocoding
using BN. Specifically, these records:

+ Returned a GRC of 22 (Invalid BIN format) or 23 (Temporary DOB BIN), or
+ Returned a GRC of 01 but had TPAD-related warnings:
+ Geo reason code was '*' suggesting a TPAD warning and
+ The TPAD conflict flag was neither blank nor 1

The records for QAQC have additional flags added:

+ *Million BIN*: Geosupport BN identified the BIN format as invalid
+ *DOB Only*: Geosupport BN identified the BIN as being temporary and only existing
+ *In TPAD*: Geosupport returned a TPAD warning greater than 1, suggesting TPAD data was found for this BIN

#### Make sure records from DOB developments database that have been issued a Certificate of Occupancy are no longer in TPAD  

Input DOB data comes from the DCP EDM-maintained Deveopments Database.
The output of this check contains records that matched a TPAD record when geocoding
using 1B. Specifically, these records have a return code of '01', with TPAD conflict flags
that are neither blank nor 1.

For more information about how TPAD matches are identified in a geosupport results,
please refer to page 782 of the [Geosupport documentation](https://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/upg.pdf?r=16b).

### Street name checks

#### Make sure street names in the Digital City Map are valid names in Geosupport

This check extracts street names from the DCM, and checks that these names can be normalized and matched with a geosupport code. 
To do so, the street name and borough from the DCM street centerline file are inputs to function 1N. 
Name - borough combinations that do not yeild a '00' return code are in the QAQC file."""
