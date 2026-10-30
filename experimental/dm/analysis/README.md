# Community district growth

A small dbt project (DuckDB, dbt v2) with population and housing by community district for the past, present, and future, plus which districts are AHFT districts.
It reads published PFF and KPDB files and has its own venv.

## Sources

`scripts/download.py` pins every file. Bump a version there to move to a new release.

| Source | Version | Used for |
|---|---|---|
| PFF decennial | 2010, 2020 | population and housing units by community district |
| KPDB `cpp_housing_growth_cd` | 2026-10-01 | projected net units 2025-2034 |
| KPDB `longform_cd_output` | 2026-10-01 | per-record units by community district |
| `dcp_housing` (edm-recipes) | 26Q2 | units built, and housing units by year from the 2020 census through 2025. Newer than the 24Q4 version KPDB 2026-10-01 used, and checked against FHGS homes added 2021-2025 instead |
| `dcp_cdboundaries`, `dcp_cdta2020` | 26c | geometry, names, CD to CDTA crosswalk |
| `dcp_housing_ahft` (edm-private) | 20261002 | AHFT rank. AHFT district means the bottom 12 of 59, the same cutoff as LIFT |
| [HPD Housing and Vacancy Survey](https://www.nyc.gov/site/hpd/about/research.page) public use files | 2021, 2023 | household size in buildings built before and after 2010, by borough. Downloaded from nyc.gov |
| [Fair Housing Growth Strategy](https://wherewelive.cityofnewyork.us/wp-content/uploads/2026/10/Fair-Housing-Growth-Strategy-Final-Report.pdf) final report | October 2026 | growth cohorts, 2016-2025 growth rates, homes in 2025, homes added 2021-2025, and 2030 targets, from the 2030 targets tables, Appendix B, and Figure 3.2. Extracted to `seeds/` |

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
| `fact_unit_completion` | completed DOB job (job_number) |
| `fact_housing_survey_household` | HVS survey year x household (control) |
| `mart_community_district_date` | borocd x date, 2010-2025 |
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
marimo edit notebooks/explore.py
```

To share the results, `python scripts/export_excel.py` writes a charts sheet, population vs housing growth, where population change went, the community district table, housing units by year (wide and long), and their column descriptions to `data/exports/Community District Growth YYYYMMDD.xlsx`, dated by the day it runs.

To change dependencies, edit `requirements.in` and run `uv pip compile requirements.in -o requirements.txt --python-version 3.13`.
