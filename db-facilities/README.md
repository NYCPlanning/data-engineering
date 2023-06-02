# db-facilities
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/NYCPlanning/db-facilities?label=version)
![Build](https://github.com/NYCPlanning/db-facilities/workflows/Build/badge.svg)

## Outputs:
| File | Description |
| ---- | ----------- |
| [facilities.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/facilities.csv) | CSV version of facDB, as of the latest build on the `develop` branch |
| [facilities.gdb.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/facilities.gdb.zip) | GeoDatabase version of facDB, as of the latest build on the `develop` branch |
| [facilities.shp.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/facilities.shp.zip) | Shapefile version of facDB, as of the latest build on the `develop` branch |
| [qc_captype.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/qc_captype.csv) | QAQC for consistency in capacity type |
| [qc_classification.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/qc_classification.csv) | QAQC for consistency in grouping information |
| [qc_diff.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/qc_diff.csv) | QAQC for change in distribution of number of records by facsubgroup/group/domain between current and previous version |
| [qc_mapped.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/qc_mapped.csv) | QAQC for change in mapped records by facdomain, facgroup, facsubgrp, factype, and datasource |
| [qc_operator.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/qc_operator.csv) | QAQC for consistency in operator information |
| [qc_oversight.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/qc_oversight.csv) | QAQC for consistency in oversight information |
| [qc_recordcounts.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/qc_recordcounts.csv) | QAQC for number of records in source vs facdb |
| [qc_subgrpbins.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/develop/latest/output/qc_subgrpbins.csv) | QAQC for number of BINs per subgroup |

## QAQC:
[Streamlit QAQC Page](https://edm-data-engineering.nycplanningdigital.com/?page=Facilities+DB)

## Development:
### Configurations
- We strongly advise that you use VScode for development of this project
- The `.devcontainer` contains all development configuration you would need, including:
    - `psql` is preinstalled
    - `python-geosupport` is installed with 21a geosupport
    - `poetry` is installed and initialized
    - `pre-commit` is also preconfigured and no additional setup is needed
- If you do not have vscode setup, don't worry, you can still develop using docker-compose
    - initialize docker-compose: `./facdb.sh init`
    - the facdb.sh script is a one to one wrapper of the python facdb cli, e.g. `./facdb.sh --help`
    - docker-compose is also used to test PR and build the dataset in github actions

### Pipeline development with the `facdb` cli
>💡 Note: the cli documentation might not always be up-to-date, to see the latest commands and features, use `facdb --help`

- Create a pipeline function under the name of the function, e.g. `dcp_colp`
- If you don't want to run every command with `poetry run`, we recommend you activate the virtual environment by `poetry shell`
- `facdb --help` to show instructions
```console
foo@bar:~$ facdb --help
Usage: facdb [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  build        Building facdb based on facdb_base
  clear        clear will clear the cached dataset created while reading a...
  dataloading  Load SQL dump datasets from data library e.g.
  export       create file export
  init         Initialize empty facdb_base table and create procedures and...
  qaqc         Running QAQC commands
  run          This function is used to execute the python portion of a...
  sql          this command will execute any given sql script against the...
```
<details><summary>Examples</summary>
    
- `facdb init` initialization of the database with `facdb_base`, functions and stored procedures
- `facdb dataloading` load supplementry datasets from data-library (e.g. `dcp_mappluto_wi`, `doitt_buildingcentroids`)
- `facdb run`
    - `facdb run -n nysed_activeinstitutions` to execute both the python and sql part specified in `datasets.yml`
    - `facdb run -n nysed_activeinstitutions --python` to execute the python part only
    - `facdb run -n nysed_activeinstitutions --sql` to execute the sql part only
    - `facdb run -n nysed_activeinstitutions --python --sql` is the same as `facdb run -n nysed_activeinstitutions`
    - `facdb run --all` to run all available data piplines defined in `datasets.yml`
- `facdb sql`
    - `facdb sql -f facdb/sql/dcp_colp.sql` to execute one script
    - `facdb sql -f facdb/sql/dcp_colp.sql -f some/other/script.sql` to execute multiple scripts
- `facdb clear`
    - `facdb clear -n nysed_activeinstitutions` to clear cache for nysed_activeinstitutions
    - `facdb clear --all` to clear all cache for all datasets
- `facdb build` build facdb from loaded source tables and produce QAQC tables
    
</details>
