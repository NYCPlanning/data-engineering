import pandas as pd

bucket = "edm-publishing"
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

readme_markdown_text = """### Source Data Info
+ Quarterly load to data-library using [dataloading workflow](https://github.com/NYCPlanning/db-gru-qaqc/blob/main/.github/workflows/dataloading.yml) included in this repo
+ **`dcp_addresspoints`**: Uploaded to edm-publishing data staging by GIS
+ **`dcp_atomicpolygons`**: Pulled from Bytes of the Big Apple
+ **`dcp_pad`**: Pulled from Bytes of the Big Apple and parsed through a [python script](https://github.com/NYCPlanning/db-data-library/blob/main/library/script/dcp_pad.py)
+ **`dcp_dcmstreetcenterline`**: Uploaded to edm-publishing data staging by GIS
+ **`dcp_saf`**: Uploaded to edm-publishing data staging by GIS. These files get read directly from the upload location, and are not loaded to data-library.
+ Requires manual reloading to data-library
+ **`doitt_buildingfootprints`**: Pulled from [OpenData](https://data.cityofnewyork.us/Housing-Development/Building-Footprints/nqwf-w8eh) and parsed through a [python script](https://github.com/NYCPlanning/db-data-library/blob/main/library/script/doitt_buildingfootprints.py). Due to instability, this data update is not get included in the batch update workflow.
+ **`dcp_developments`**: Gets published to data-library upon rebuilding using a [workflow](https://github.com/NYCPlanning/data-engineering/blob/main/.github/workflows/developments_publish.yml) in the db-developments repo. No other update necessary.

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
