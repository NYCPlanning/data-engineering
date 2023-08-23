# db-zap-opendata

Workflows for:

- creating subset of ZAP data that's on open data
- creating MapZAP

## ZAP Open Data

This repo archives and processes the data in DCP's customer relationship management (CRM) system used for ZAP projects.

Two versions of the CRM data are archived to Digital Ocean S3 file storage, Google Cloud Storage and BigQuery:

- Raw CRM data
- A subset of records flagged for public visibility with recoded values

### Run a ZAP Open Data export script

1. Open repo in the defined dev container

2. Run a ZAP Pull

    ```bash
    python -m src.runner <name of the entity>
    ```

    e.g.

    ```bash
    python -m src.runner dcp_projects
    ```

## MapZAP

This repo is used to build MapZAP, a dataset of ZAP project records with spatial data.

A ZAP project has a geometry assigned to it from either of two sources:

- Based on the ULURP numbers in a ZAP Project, a geometry from the internal version of the Zoning Map Amendment represents the project
- Based on the BBLs in a ZAP project when it was referred for review (Project certified referred year), a version of PLUTO from that year is chosen and used to find and aggregate BBL geometries to represent the project.

### Data sources

- ZAP Projects
- ZAP Project BBLs
- MapPLUTO (versions from 2002 - 2022)

> Note: All source data is in BigQuery as a result of the ZAP Open Data export workflows in this repo

### Build process (locally only)

1. Clone the repo and create `.devcontainer/.env`

2. Open the repo in the defined dev container in VS code

3. Run the following dbt commands to build BigQuery dataset named `dbt_mapzap_dev_YOUR_DBT_USER`

    ```bash
    dbt debug
    dbt deps
    dbt seed --full-refresh
    dbt run
    dbt test
    ```

    > Note: Use of dbt requires a personal keyfile at `.dbt/bigquery-dbt-dev-keyfile.json`. This can be generated and shared by a Google Cloud Platform admin.

4. Review outputs in BigQuery and export as CSV to Google Cloud Storage

## Dev

> Note: set the environmental variables in `.env` according to `example.env`.

### Using dbt

Setup

```bash
dbt deps
dbt debug
```

Building tables

```bash
dbt seed --full-refresh
dbt run
dbt test
```

Building docs

```bash
dbt docs generate
dbt docs serve
```

### Develop dbt

Run pre commit checks for all model and config files:

```bash
pre-commit run --all-files
```

  > Note: This is configured by `.pre-commit-config.yaml` and will run `dbt compile` and `dbt docs generate`

Run a single model:

```bash
dbt run --select int_zap_project_bbls
```

Run a single model and it's parent models:

```bash
dbt run --select +int_zap_project_bbls
```

Run a single model, its children, and the parents of those children:

```bash
dbt run --select int_zap_project_bbls@
```

### Notes for in-progress MapZAP work

- For use in CEQR Type II analysis by Planning Support team:
  - For the ZAP data pull, specifically, we propose to narrow down from all records using the criteria below. Assuming you want to pull and join all ZAP BBL records so this work is useful for others, this use case will need the fields in parentheses below as columns so I can filter down the set:
    - Certified/referred date on or after 2/1/11 (certified/referred field from project entity)
    - CEQR number contains data (CEQR number from project entity)
    - CEQR Type does not equal Type II (CEQR Type from project entity)
    - Public status equals completed (Public Status from project entity)
    - Project Status does not equal Record Closed, Terminated, Terminated-Applicant Unresponsive, or Withdrawn-Other (Project - Status from project entity)
    - BBL is not located within a Special Coastal Risk District
    - Existing zoning district(s)
    - Proposed zoning district(s)\
    - [below this, I'm confused]
    - For all projects with a rezoning action, was it a rezoning from an M district to an R district? (Will filter out those  where - the answer is yes)
    - Applicant Type does not equal DCP (Applicant Type from project entity)
  - Additionally, the data pull I’ve been using pulls data into the following columns for later use:
    - Project ID
    - Project Name
    - Lead Division
    - Actions
    - Project Status
    - BBL
    - CEQR Type
    - WRP Review Required
    - FEMA Flood Zone V
  - Lastly, would it be possible to auto calculate these two into the records? If not, we can do it later.
    - Number of unique blocks per project
    - For projects with a rezoning action, sum the total rezoned area
