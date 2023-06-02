# City Owned and Leased Properties Database ![CI](https://github.com/NYCPlanning/db-colp/workflows/CI/badge.svg)

Various city authorities own or lease a large inventory of properties. This repo contains code to create a database of all city owned and leased properties, as required under Section 204 of the City Charter. The database contains information about each property's use, owning/leasing agency, location, and tenent agreements. This repo is a refactoring of the dataset found on [Bytes of the Big Apple](https://www1.nyc.gov/site/planning/about/publications/colp.page), and is currently in development.

The input data for COLP is the Integrated Property Information System (IPIS), a real estate database maintained by the Department of Citywide Administrative Services (DCAS).

## Outputs
| File | Description |
| ---- | ----------- |
| [output.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/output.zip) | Zipped directory containing all files below |
| [colp.shp.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/colp.shp.zip) | Shapefile version COLP database, only including records with coordinates |
| [colp.gdb.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/colp.gdb.zip) | GeoDatabase version COLP database, only including records with coordinates |
| [colp.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/colp.csv) | CSV version COLP database, only including records with coordinates |
| [ipis_modified_hnums.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/ipis_modified_hnums.csv) | QAQC table of records with modified house numbers |
| [ipis_modified_names.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/ipis_modified_names.csv) | QAQC table of records with modified parcel names |
| [usetype_changes.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/usetype_changes.csv) | QAQC table of version-to-version changes in the number of records per use type |
| [modifications_applied.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/modifications_applied.csv) | Table of manual modifications that were applied |
| [modifications_not_applied.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/modifications_not_applied.csv) | Table of manual modifications that existed in the modifications table, but failed to get applied |
| [ipis_unmapped.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/ipis_unmapped.csv) | QAQC table of unmappable input records |
| [ipis_colp_geoerrors.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/ipis_colp_geoerrors.csv) | QAQC table of addresses that return errors (or warnings type 1-9, B, C, I, J) from 1B |
| [ipis_sname_errors.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/ipis_sname_errors.csv) | QAQC table of addresses that return streetname errors (GRC is 11 or EE) from 1B |
| [ipis_hnum_errors.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/ipis_hnum_errors.csv) | QAQC table of addresses that return out-of-range address errors (GRC is 41 or 42) from 1B |
| [ipis_bbl_errors.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/ipis_bbl_errors.csv) | QAQC table of records where address isn't valid for input BBL |
| [ipis_cd_errors.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/qaqc/ipis_cd_errors.csv) | QAQC table of mismatch between IPIS community district and PLUTO |
| [version.txt](https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/main/latest/output/version.txt) | Build date |

## Additional Resources
Look-up tables for agency abbreviations and use types are availaible in CSV form under [`/resources`](https://github.com/NYCPlanning/db-colp/tree/main/resources)

## Building COLP
There are currently two methods to create colp:
1. via a `push` event, make sure you include `[build]` in your commit message to trigger a build. This workflow can be triggered on all branches.
2. via a `workflow_dispatch` event. Head to the `actions` tab of the repo and click **Run Workflow** under the **Build** section. 
