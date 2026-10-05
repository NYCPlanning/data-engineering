# Capital Projects Database (CPDB)

The Capital Projects Database (CPDB) captures key data points on potential, planned, and ongoing capital projects sponsored or managed by a capital agency in New York City.

Each version is built from one release of OMB's Capital Commitment Plan, plus FISA budget data and Checkbook NYC spending. See the [Capital Planning Glossary](../../docs/glossary-capital-planning.md) for the terms, the budget cycle, and how CPDB versions are named.

## Important files

- [`recipe.yml`](recipe.yml): input datasets and the version being built
- [`cpdb.sh`](cpdb.sh): build entrypoint (`./cpdb.sh build`), run by the [build workflow](../../.github/workflows/cpdb_build.yml)
- [`models/`](models/): dbt models
- [`seeds/`](seeds/): dbt seeds, documented in [`seeds/_seeds.yml`](seeds/_seeds.yml)

## Links

- CPDB on Open Data
  - [Projects](https://data.cityofnewyork.us/d/fi59-268w)
  - [Mapped Projects (Points)](https://data.cityofnewyork.us/d/h2ic-zdws)
  - [Mapped Projects (Polygons)](https://data.cityofnewyork.us/d/9jkp-n57r)
  - [Commitments](https://data.cityofnewyork.us/d/djxg-kcfi)
- [Data dictionary](https://s-media.nyc.gov/agencies/dcp/assets/files/excel/data-tools/bytes/cpdb_data_dictionary.xlsx)
- [DCP datasets](https://www.nyc.gov/content/planning/pages/resources?search=capital#datasets), where the old Bytes of the Big Apple page now redirects
- [Wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Product:-CPDB) with more in-depth information
