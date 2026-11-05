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

Housing units through 2025 come from `dcp_housing` 26Q2. KPDB 2026-10-01 was built on 24Q4, so only its projection is used, added to the end of 2024 from 26Q2.
Neither source projects population.

The FHGS seeds are committed, so a build doesn't need the PDF. To extract them again, download it and run `scripts/extract_fhgs.py`, which needs `pdftotext` (poppler):

```bash
curl -o data/sources/fhgs.pdf https://wherewelive.cityofnewyork.us/wp-content/uploads/2026/10/Fair-Housing-Growth-Strategy-Final-Report.pdf
python scripts/extract_fhgs.py data/sources/fhgs.pdf
```

## Models

| Model | Grain |
|---|---|
| `dim_community_district` | borocd (59 districts and 12 joint interest areas), with geometry |
| `dim_kpdb_record_community_district` | KPDB record x borocd |
| `dim_nta` | 2020 neighborhood tabulation area (nta2020), with geometry |
| `dim_census_tract` | 2020 census tract (boroct2020), with geometry |
| `dim_borough` | borough, with TCOL rates |
| `dim_tcol_family_type` | TCOL family type, with its median threshold and rates |
| `dim_transit_zone` | transit zone, with geometry |
| `dim_school_subdistrict` | school subdistrict (school_subdistrict_id), with 2036 utilization and geometry |
| `dim_community_district_school_subdistrict` | borocd x school_subdistrict_id, the piece of a district in each subdistrict |
| `dim_nta_neighbor` | nta2020 x neighbor_nta2020, every pair of NTAs that touch |
| `dim_community_district_transit_zone` | borocd x transit_zone, the piece of a district in each zone |
| `dim_ms4_drainage_area` | MS4 drainage area, every column but geometry, with geometry |
| `dim_ms4_outfall` | MS4 outfall (outfall_id), with geometry |
| `dim_community_district_ms4_drainage` | borocd x drains_to_type, the piece of a district in each type of MS4 drainage |
| `dim_wastewater_facility` | wastewater facility (WRRF), with its functions and latest flow |
| `fact_wastewater_facility_month` | wastewater facility x month of DEP's performance reports |
| `dim_community_district_stormwater_flooding` | borocd x flooding_category, with the district's flooding geometry |
| `dim_community_district_stormwater_flooding_frame` | borocd x flooding_category, the flooding in the district's map frame |
| `dim_community_district_floodplain_2080` | borocd, with its share in the 2080s 100-year floodplain and the floodplain in its map frame |
| `fact_unit_completion` | completed DOB job (job_number) |
| `fact_housing_survey_household` | HVS survey year x household (control) |
| `mart_community_district_date` | borocd x date, 2010-2025 |
| `mart_nta_date` | nta2020 x date, 2010-2025 |
| `mart_borough_survey_year` | borough x HVS survey year |
| `population_vs_housing_growth` (product, Excel) | borocd, the 59 community districts |
| `population_change_components` (product, Excel) | borocd, the 59 community districts |

Column docs are in `models/core/_core_models.yml`.

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
