# Community district growth

A small dbt project (DuckDB, dbt v2) with population and housing by community district for the past, present, and future, plus which districts are AHFT districts.
It reads published PFF and KPDB files and has its own venv.

## Sources

`scripts/download.py` pins every file. Bump a version there to move to a new release.

| Source | Version | Used for |
|---|---|---|
| PFF decennial | 2010, 2020 | population and housing units by community district, NTA, and census tract |
| PFF ACS | 2020-2024 | household income, family income, and poverty by census tract, NTA, CDTA, and citywide |
| KPDB `cpp_housing_growth_cd`, `cpp_housing_growth_nta` | 2026-10-01 | projected net units 2025-2034 |
| KPDB `longform_cd_output`, `longform_nta_output` | 2026-10-01 | per-record units by community district and NTA |
| `dcp_housing` (edm-recipes) | 26Q2 | units built, and housing units by year from the 2020 census through 2025. Newer than the 24Q4 version KPDB 2026-10-01 used, and checked against FHGS homes added 2021-2025 instead |
| `dcp_cdboundaries`, `dcp_cdta2020`, `dcp_nta2020`, `dcp_ct2020` | 26c | geometry, names, CD to CDTA crosswalk, NTAs, census tracts |
| `dcp_transit_zones` | 20251209 | transit zones, which set parking requirements |
| `doe_school_subdistricts` | 20250901 | SCA's school subdistricts |
| DOT Vision Zero View on NYC Open Data: [priority corridors](https://data.cityofnewyork.us/d/36nr-7fbp), [priority intersections](https://data.cityofnewyork.us/d/tmt9-43em), Street Improvement Project [corridors](https://data.cityofnewyork.us/d/if4c-w48d) and [intersections](https://data.cityofnewyork.us/d/shr7-eqdc) | downloaded November 2026, not versioned | pedestrian safety priority streets and DOT safety projects by community district |
| [DEP MS4 map](https://data.cityofnewyork.us/d/j57c-rqtq) (NYC Open Data) | August 1, 2020 | storm sewer drainage areas and outfalls. Its waste facility, stormwater permit, and Bluebelt layers stop at staging for now |
| [DEP Map of the New York City Wastewater System](https://www.nyc.gov/assets/dep/downloads/pdf/water/wastewater/nyc-wastewater-system-map.pdf) | 2025 | the table of Wastewater Resource Recovery Facilities (WRRFs) and their functions. Extracted to `seeds/` by `scripts/extract_dep_wastewater_map.py` |
| [DEP Wastewater Treatment Plant Performance Data](https://data.cityofnewyork.us/d/hgue-hj96) (NYC Open Data) | 2015 to July 2025 | monthly flow and effluent by WRRF, from every report file on the page |
| [DEP NYC Stormwater Flood Maps](https://data.cityofnewyork.us/d/9i7c-xyvv) (NYC Open Data) | Moderate Flood (2.13 inches per hour) with 2050 Sea Level Rise | stormwater and 2050 high tide flooding by community district |
| [Sea Level Rise Maps (2080s 100-year Floodplain)](https://data.cityofnewyork.us/d/ek8y-fsqz) (NYC Open Data) | 2080s, 58 inches of sea level rise | coastal floodplain by community district |
| `dcp_housing_ahft` (edm-private) | 20261002 | AHFT rank. AHFT district means the bottom 12 of 59, the same cutoff as LIFT |
| [HPD Housing and Vacancy Survey](https://www.nyc.gov/site/hpd/about/research.page) public use files | 2021, 2023 | household size in buildings built before and after 2010, by borough. Downloaded from nyc.gov |
| [Fair Housing Growth Strategy](https://wherewelive.cityofnewyork.us/wp-content/uploads/2026/10/Fair-Housing-Growth-Strategy-Final-Report.pdf) final report | October 2026 | growth cohorts, 2016-2025 growth rates, homes in 2025, homes added 2021-2025, and 2030 targets, from the 2030 targets tables, Appendix B, and Figure 3.2. Extracted to `seeds/` |
| [NYC True Cost of Living report](https://www.nyc.gov/assets/equity/downloads/pdf/2026-NYC-TCOL-Report.pdf) | 2026 (2022 data) | TCOL thresholds by family type, and the share of residents not meeting them by group and borough. Extracted to `seeds/` by `scripts/extract_tcol.py` |
| AHFT schools workbook (`AHFT Schools Analysis.xlsx`, not public) | No Action, taken to be 2036 | primary and intermediate school enrollment, seats, and utilization for the 30 subdistricts related to AHFT districts, from its AHFT_ONLY sheet. Extracted to `seeds/` by `scripts/extract_ahft_schools.py` |
| SCA school capacity projects CSV (with the AHFT schools workbook, not public, copied into `data/sources/sca_capacity_projects.csv` by hand) | June 2026 | planned school capacity projects with seats, start dates, and subdistricts |
| MTA on data.ny.gov via edm-recipes: `mta_subway_stations`, `mta_subway_service_lines`, `mta_subway_entrances`, `mta_rail_stations` | 20261107, 20251208, 20251205, 20261107 | subway and SIR stations with routes and ADA status, service lines, entrances, and LIRR and Metro-North stations |
| MTA bus stops on data.ny.gov via edm-recipes: `mta_bus_stops` | 20261016 | bus stops in the schedule bundle in effect |
| MTA capital program on data.ny.gov via edm-recipes: `mta_capital_project_locations` ([Capital Dashboard Project Locations](https://data.ny.gov/d/wcsa-vkhf)), `mta_capital_project_details` ([Capital Project Details](https://data.ny.gov/d/9hy6-8j6t)), and `mta_capital_project_budgets` ([Capital Project Budgets](https://data.ny.gov/d/f6fd-xfps)) | 20260508, 20261023, 20261023 | capital project locations by plan, the stage and dates of the projects MTA details, and the ACEP codes that tie the two |
| DDC infrastructure capital projects via edm-recipes: `ddc_capitalprojects_infrastructure` (a shapefile DDC sends) | 20260701 | street segments of DDC sewer, water main, green infrastructure, and roadway projects, with phase and dates |
| Capital Projects Database (CPDB, DCP) via edm-publishing: `cpdb_projects.csv` and `analysis/projects_by_communitydist.csv` | 26exec | city capital projects with dollars by stage, and mapped projects' planned commitments by community district |
| Proposed Interborough Express via edm-recipes: `mta_ibx_stations`, `mta_ibx_service_lines` (HDR's public-facing IBX design layers on ArcGIS Online) | 20261028, 20261030 | proposed IBX stations and track alignment. Subject to change |
| NYC Ferry GTFS via edm-recipes (`edc_nycferry_stops`, `_routes`, `_trips`, `_stop_times`) and `dot_ferryterminals` | 20261108, 20240805 | NYC Ferry landings and the Staten Island Ferry terminals |
| DEP's public ArcGIS Online layer via edm-recipes: `dep_bluebelt_bmps` | 20240325 | Bluebelt BMPs |
| FacDB via edm-recipes: `dcp_facilities` | 26v1 | school points. FacDB geocodes DOE's LCGMS list |
| DCP 2020 census blocks via edm-recipes: `dcp_cb2020` | 26c | block shapes, to put 2020 block populations in districts |
| NYC Parks properties via edm-recipes: `dpr_parksproperties` | 20261016 | park boundaries and types |
| NYC Parks Walk to a Park service area for CEQR via edm-recipes: `dpr_walk_to_a_park` | 20261028 | tax lots within walking distance of a park |
| NYC Parks Capital Project Tracker feed (the raw file behind `dpr_capitalprojects` 20260625 in edm-recipes' inbox, copied into `data/sources/dpr_capitalprojects.json` by hand) | June 2026 | park capital projects with phase, dates, funding, and locations |
| NYC Parks Vital Parks Explorer area stats (CSV downloaded from the [Explorer](https://www.nycgovparks.org/about/vital-parks/explorer), copied into `data/sources/dpr_vital_parks_area_stats.csv` by hand) | November 2026 | percentages by park element for the city, community districts, council districts, and NYS disadvantaged communities |
| NYC Parks walk times to open space (GeoJSON from Parks, not public, copied into `data/sources/dpr_open_space_walk_segments.geojson` by hand) | November 2026 | residents and walk minutes to open space, by street segment |
| DEP WRRF drainage areas shapefile (internal to DCP, copied into `data/sources/dcp_wrrf_drainage_areas.zip` by hand) | unknown | wastewater treatment plant sewersheds. Used over DEP's public `dep_wwtp_drainage_areas` layer, which puts Rikers Island in Hunts Point. DEP's 2025 wastewater system map has it in Bowery Bay, as this file does |
| DEP separate sewers and direct drainage shapefile (internal to DCP, copied into `data/sources/dcp_separate_sewers_direct_drainage.zip` by hand) | revised 2001-2008 | separate sewer and direct drainage areas. No combined class |
| DEP Bluebelt properties shapefile (internal to DCP, copied into `data/sources/dcp_bluebelt_properties.zip` by hand) | about 2014 | DEP-owned Bluebelt parcels on Staten Island |
| Access to land area (no decay) shapefile (from CEQR work, not public, copied into `data/sources/transit_land_access.zip` by hand) | November 2026 | acres reachable by transit in 60 minutes, its percentile, and 20 percentile bands, by 2020 census block |

Housing units through 2025 come from `dcp_housing` 26Q2. KPDB 2026-10-01 was built on 24Q4, so only its projection is used, added to the end of 2024 from 26Q2.
Neither source projects population.

The FHGS seeds are committed, so a build doesn't need the PDF. To extract them again, download it and run `scripts/extract_fhgs.py`, which needs `pdftotext` (poppler):

```bash
curl -o data/sources/fhgs.pdf https://wherewelive.cityofnewyork.us/wp-content/uploads/2026/10/Fair-Housing-Growth-Strategy-Final-Report.pdf
python scripts/extract_fhgs.py data/sources/fhgs.pdf
```

## Models

Core models are grouped by domain in `models/core/`, with intermediate models in matching folders of `models/intermediate/`. Each folder has its models' column docs.

### Geography (`geography/`)

| Model | Grain |
|---|---|
| `dim_community_district` | borocd (59 districts and 12 joint interest areas), with geometry |
| `dim_nta` | 2020 neighborhood tabulation area (nta2020), with geometry |
| `dim_census_tract` | 2020 census tract (boroct2020), with geometry |
| `dim_borough` | borough, with TCOL rates |
| `dim_nta_neighbor` | nta2020 x neighbor_nta2020, every pair of NTAs that touch or are across less than a quarter mile of water |

### Residents and growth (`residents_growth/`)

| Model | Grain |
|---|---|
| `mart_community_district_date` | borocd x date, 2010-2025 |
| `mart_nta_date` | nta2020 x date, 2010-2025 |
| `fact_unit_completion` | completed DOB job (job_number) |
| `dim_kpdb_record_community_district` | KPDB record x borocd |
| `fact_housing_survey_household` | HVS survey year x household (control) |
| `mart_borough_survey_year` | borough x HVS survey year |
| `dim_tcol_family_type` | TCOL family type, with its median threshold and rates |

### Transportation (`transportation/`)

| Model | Grain |
|---|---|
| `dim_transit_zone` | transit zone, with geometry |
| `dim_community_district_transit_zone` | borocd x transit_zone, the piece of a district in each zone |
| `dim_subway_station` | subway and SIR station (gtfs_stop_id), with geometry |
| `dim_subway_service_line` | subway service line shape, with geometry |
| `dim_subway_entrance` | subway entrance or exit, with geometry |
| `dim_rail_station` | LIRR and Metro-North station in NYC, with geometry |
| `dim_ibx_station` | proposed IBX station, with geometry |
| `dim_ibx_track` | proposed IBX track centerline, with geometry |
| `dim_ferry_landing` | NYC Ferry landing or Staten Island Ferry terminal, with geometry |
| `dim_vision_zero_priority_corridor` | Vision Zero pedestrian priority corridor, with geometry |
| `dim_vision_zero_priority_intersection` | Vision Zero pedestrian priority intersection, with geometry |
| `dim_street_improvement_project` | DOT Street Improvement Project (corridor or intersection), with geometry |
| `dim_bus_stop` | MTA bus stop where riders can board (bus_stop_id), as a point |
| `dim_mta_capital_project_location` | acep x location_number, a location of an MTA capital project, as a point |
| `dim_mta_capital_project` | MTA capital project in the Capital Project Details dataset (project_id), with stage and dates |
| `dim_mta_capital_project_acep` | project_id x acep, the ACEP codes in a project's budget |
| `dim_community_district_mta_capital_project` | borocd x acep, an MTA capital project with a location in the district, with one point for it there |
| `dim_transit_access_block` | 2020 census block in the access to land area file, with geometry |

### Schools (`schools/`)

| Model | Grain |
|---|---|
| `dim_school_subdistrict` | school subdistrict (school_subdistrict_id), with 2036 utilization and geometry |
| `dim_community_district_school_subdistrict` | borocd x school_subdistrict_id, the piece of a district in each subdistrict |
| `dim_school` | school in DOE's LCGMS list (school_id), as a point, from FacDB 26v1 |
| `dim_school_capacity_project` | SCA school capacity project, with seats, start date, subdistrict, and point |

### Water (`water/`)

| Model | Grain |
|---|---|
| `dim_ms4_drainage_area` | MS4 drainage area, every column but geometry, with geometry |
| `dim_ms4_outfall` | MS4 outfall (outfall_id), with geometry |
| `dim_community_district_ms4_drainage` | borocd x drains_to_type, the piece of a district in each type of MS4 drainage |
| `dim_bluebelt_bmp` | Bluebelt best management practice (bmp_id), as a point, as of March 2024 |
| `dim_bluebelt_parcel` | polygon of DEP-owned Bluebelt land, about 2014, with the BMPs inside it |
| `dim_wastewater_facility` | wastewater facility (WRRF), with its functions, latest flow, and location |
| `dim_ddc_infrastructure_project` | DDC infrastructure capital project (ddc_project_id), with its street segment lines |
| `fact_wastewater_facility_month` | wastewater facility x month of DEP's performance reports |
| `dim_wastewater_sewershed` | wastewater treatment plant sewershed (wastewater_facility_id), with geometry |
| `dim_community_district_wastewater_sewershed` | borocd x wastewater_facility_id, the piece of a district in each sewershed |
| `dim_sewer_area` | kind of sewer area (separate or direct drainage), with geometry |
| `dim_community_district_sewer_area` | borocd x sewer_area_type, the piece of a district in each kind of sewer area |
| `dim_community_district_stormwater_flooding` | borocd x flooding_category, with the district's flooding geometry |
| `dim_community_district_stormwater_flooding_frame` | borocd x flooding_category, the flooding in the district's map frame |
| `dim_community_district_floodplain_2080` | borocd, with its share in the 2080s 100-year floodplain and the floodplain in its map frame |

### Open space (`open_space/`)

| Model | Grain |
|---|---|
| `dim_park` | NYC Parks property (park_id), with geometry |
| `dim_walk_to_a_park_area` | polygon of Parks' Walk to a Park service area, with geometry |
| `dim_park_capital_project` | Parks capital project (tracker_id), with phase, dates, and funding |
| `dim_park_capital_project_location` | tracker_id x location_number, with a point where the feed has one |
| `dim_open_space_walk_segment` | street segment in Parks' open space walk file (segment_id), with residents, walk minutes, and line |

### Capital (`capital/`)

| Model | Grain |
|---|---|
| `dim_cpdb_project` | CPDB capital project (maprojid), every agency |
| `dim_community_district_cpdb_project` | borocd x maprojid, a mapped CPDB project's planned commitments in the district |

### Products

| Model | Grain |
|---|---|
| `population_vs_housing_growth` (product, Excel) | borocd, the 59 community districts |
| `population_change_components` (product, Excel) | borocd, the 59 community districts |

## Running it

From this folder, with direnv loading the repo's root `.env` for S3 credentials:

```bash
uv venv --python 3.13
uv pip install --python .venv/bin/python --requirement requirements.txt

python scripts/download.py
dbt build
marimo edit notebooks/explore_growth.py
```

`notebooks/explore_demographics.py` zooms into one community district's census tracts.
A district's tracts are the tracts in its CDTA, since tracts don't nest in districts.

`notebooks/neighborhood_capital_profiles.py` has one section each for a district's residents, growth, and infrastructure. Infrastructure is only a list of TODO subsections for now.

To share the results, `python scripts/export_excel.py` writes a charts sheet, population vs housing growth, where population change went, the community district table, housing units by year (wide and long), and their column descriptions to `data/exports/Community District Growth YYYYMMDD.xlsx`, dated by the day it runs.

To change dependencies, edit `requirements.in` and run `uv pip compile requirements.in -o requirements.txt --python-version 3.13`.
