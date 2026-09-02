# Equitable Development Data Explorer (EDDE)

EDDE is a set of New York City community-level data related to demographics, housing, and quality of life.

The [Racial Impact Study Coalition](https://racialimpactnyc.wordpress.com/our-story/) was the force behind it's creation.

DCP Housing is the product owner, and Data Engineering's point-person has (as of this writing in 2025) been Winnie Shen.

# Annual Refresh Process

Each year, stakeholders (DCP Population, HPD, DOHMH, etc.) send updated source files. Updating
EDDE for a new year is three steps:

## 1. Drop the new files into `products/edde/resources/`

Each stakeholder file replaces the existing file at the same relative path, in the same format
(sheet names, column names) as the prior year's - the loaders in `resources.py` expect an exact
match. If a format changes, update the corresponding loader in `resources.py` too.

| Relative path (under `products/edde/resources/`) | Indicator(s) |
|---|---|
| `housing_production/2010_census_housing_units_by_2020_NTA.csv` | 4.01 |
| `decennial_census_data/EDDE_Census00-10-20_MUTU.xlsx` | 1.01 |
| `ACS_PUMS/EDDE_Census2000PUMS.xlsx` | 1.02–1.04, 2.01, 2.04–2.06, 3.01–3.04, 3.06, 3.08, 5.10, 5.11 (2000 vintage) |
| `ACS_PUMS/EDDE_ACS2008-2012.xlsx` | same set as above, prior ACS vintage |
| `ACS_PUMS/EDDE_ACS2020-2024.xlsx` | same set as above, current ACS vintage |
| `quality_of_life/education_math_ela_grad.xlsx` | 5.12, 5.13 |
| `quality_of_life/non_fatal_assault_hospitalizations.csv` | 5.18 |
| `quality_of_life/pedestrian_hospitalizations.csv` | 5.17 |
| `quality_of_life/dohmh_death_rate_and_overdose.xlsx` | 5.03, 5.04, 5.05 |
| `quality_of_life/diabetes_self_report/diabetes_self_report_processed_<year>.xlsx` | 5.01, 5.02 (currently disabled) |
| `quality_of_life/deaths_by_race_and_puma.xlsx` | 5.06 (currently disabled) |
| `quality_of_life/pop_census_aggregations.csv` | 5.06 denominator (currently disabled) |
| `quality_of_life/transportation.xlsx` | 5.08, 5.09, 5.14 |
| `housing_security/nychvs.xlsx` | 3.05, 3.07 |
| `housing_security/nycha_tenants.xlsx` | 3.11 (NYCHA portion), 3.12 |
| `housing_security/hpd_housing_lottery.xlsx` | 3.13, 3.14 |

A few indicators (e.g. 3.09 evictions, 3.11's HPD portion, 4.02, 4.03) instead come from datasets
ingested via `dcpy.lifecycle.ingest` and declared in `recipe.yml`'s `inputs.datasets` - update
those dataset versions there rather than dropping a file into `resources/`.

## 2. Update `recipe.yml` and `indicators.csv`

Bump the year variables under `recipe.yml`'s `custom:` block (`ACS_CURRENT_YEAR_BAND`, `nycha.year`,
`education.year`, etc.) and the corresponding rows in `indicators.csv` (the `earliest`/`middle`/
`current` start/end years shown in each table's vintage header - see `indicators.py` for how these
get loaded and templated into `packager/site_conf_templates/templates/*.json`).

## 3. Run the build

```bash
python -m dcpy.lifecycle.builds.plan recipe --recipe-path products/edde/recipe.yml --output-path products/edde/recipe.lock.yml
python -m dcpy lifecycle builds build run --product edde --build-dir <build-dir>
```

This runs `build` → `data_checks` → `qa` → `package` in sequence (see `recipe.yml`'s
`stage_config.builds.build.commands`), producing both the per-year indicator CSVs
(`<build-dir>/dataset_files/`) and the final packaged site artifacts
(`<build-dir>/attachments/` - site config JSON, resolved per-district pages, district XLSX exports).

## Links

[EDDE application](https://equitableexplorer.planning.nyc.gov/map/data/district)
[EDDE legislation](https://legistar.council.nyc.gov/MeetingDetail.aspx?ID=829692&GUID=2F8FEE3A-D5AE-4E32-9BF5-2D935AD6C868&Options=&Search=)
