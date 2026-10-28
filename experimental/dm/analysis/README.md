# Community district growth

A small dbt project (DuckDB, dbt v2) with population and housing by community district for the past, present, and future, plus which districts are AHFT districts.
It reads published PFF and KPDB files and has its own venv.

## Sources

`scripts/download.py` pins every file. Bump a version there to move to a new release.

| Source | Version | Used for |
|---|---|---|
| PFF decennial | 2010, 2020 | population and housing units by community district |
| PFF ACS | 2020-2024 5-year | population and housing units by CDTA, mapped 1:1 to community districts |
| KPDB `cpp_housing_growth_cd` | 2026-10-01 | housing units at the end of 2020 and 2024, projected through 2034 |
| KPDB `longform_cd_output` | 2026-10-01 | per-record units by community district |
| `dcp_housing` (edm-recipes) | 24Q4 | units built between the 2010 and 2020 censuses. The same version KPDB 2026-10-01 used, so bump them together |
| `dcp_cdboundaries`, `dcp_cdta2020` | 26c | geometry, names, CD to CDTA crosswalk |
| `dcp_housing_ahft` (edm-private) | 20261002 | AHFT rank. AHFT district means the bottom 12 of 59, the same cutoff as LIFT |
| [Fair Housing Growth Strategy](https://wherewelive.cityofnewyork.us/wp-content/uploads/2026/10/Fair-Housing-Growth-Strategy-Final-Report.pdf) final report | October 2026 | growth cohorts, 2016-2025 growth rates, homes in 2025, homes added 2021-2025, and 2030 targets, from the 2030 targets tables, Appendix B, and Figure 3.2. Extracted to `seeds/` |

KPDB 2026-10-01 counts completions from `dcp_housing` 24Q4, so "present" is the end of 2024.
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

To change dependencies, edit `requirements.in` and run `uv pip compile requirements.in -o requirements.txt --python-version 3.13`.
