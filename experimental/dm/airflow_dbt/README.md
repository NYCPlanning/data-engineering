# Airflow dbt POC

This is a proof-of-concept for using Airflow to orchestrate dbt projects.

## Setup

```bash
# from this directory
uv venv
source .venv/bin/activate
uv pip sync requirements.txt
```

## Run Airlfow in standalone mode

```bash
# from this directory
export AIRFLOW_HOME="$(pwd)"/.airflow
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)"/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
```

Navigate to `http://localhost:8080` in a browser.

The password for the admin user be in `.airflow/simple_auth_manager_passwords.json.generated`.

## Resources

- Docs: <https://airflow.apache.org/docs/apache-airflow/stable/index.html#>
- `apache-airflow` python package GitHub repo: <https://github.com/apache/airflow/tree/main>
  - `INSTALLING.md`: <https://github.com/apache/airflow/blob/main/INSTALLING.md>
- Best practices: <https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html>
- Providers index: <https://airflow.apache.org/docs/#providers-packages-docs-apache-airflow-providers-index-html>
  - `postgres` provider: <https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/index.html>
  - `azure` provider: <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/index.html>
- Production deployment: <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html>
- `astronomer-cosmos` python package: <https://github.com/astronomer/astronomer-cosmos>
