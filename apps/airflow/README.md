# Airflow POC

This is a proof-of-concept setup for using Airflow as our orchestration tool

## Setup

```bash
cd apps/airflow/
uv venv
source .venv/bin/activate
uv pip sync requirements.txt
```

## Run Airlfow in standalone mode

```bash
airflow standalone
```

Navigate to `http://localhost:8080` in a browser.

Password for the admin user will likely be generated in `/Users/LOCAL_USERNAME/airflow/simple_auth_manager_passwords.json.generated`.

## Resources

- Docs: <https://airflow.apache.org/docs/apache-airflow/stable/index.html#>
- `apache-airflow` python package GitHub page: <https://github.com/apache/airflow/tree/main>
  - `INSTALLING.md`: <https://github.com/apache/airflow/blob/main/INSTALLING.md>
- Best practices: <https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html>
- Providers index: <https://airflow.apache.org/docs/#providers-packages-docs-apache-airflow-providers-index-html>
  - `postgres` provider: <https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/index.html>
  - `azure` provider: <https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/index.html>
- Production deployment: <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html>
