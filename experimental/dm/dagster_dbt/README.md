# Airflow dbt POC

This is a proof-of-concept for using Dagster to orchestrate dbt projects.

## Setup

```bash
uv venv
uvx create-dagster@latest project dagster-project
cd dagster-project
source .venv/bin/activate
uv add dagster-dbt dbt-duckdb
dg --version
```

## XXX

```bash
export DAGSTER_HOME="$(pwd)"/.dagster_home
# get the example jaffle shop dbt project
git clone --depth=1 https://github.com/dbt-labs/jaffle_shop.git dbt && rm -rf dbt/.git

# scaffold a dbt component definition, will generate a defs.yaml
dg scaffold defs dagster_dbt.DbtProjectComponent dbt_project_a --project-path "../dbt_jaffle_shop"

# load and validate Dagster definitions
dg check defs
dg list defs

# start local Dagster instance
dg dev
```

## Resources

- Docs: <https://docs.dagster.io/>
- `dagster` python package GitHub repo: <https://github.com/dagster-io/dagster>
- Dagster dbt library docs: <https://docs.dagster.io/integrations/libraries/dbt>
- Dagster integration with dbt example from docs: <https://docs.dagster.io/examples/full-pipelines/dbt>
- dbt example in Dagster repo: <https://github.com/dagster-io/dagster/tree/master/examples/docs_projects/project_dbt>
- Self-hosting Dagster docs: <https://docs.dagster.io/deployment/oss/oss-deployment-architecture>
- Dagster Labs's internal project GitHub repo: <https://github.com/dagster-io/dagster-open-platform/tree/main>
