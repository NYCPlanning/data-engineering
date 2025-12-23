from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    dag_id="minimal_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="*/5 * * * *",  # Runs every 5 minutes
    catchup=False,
    tags=["example"],
)
def MinimalDag():
    """
    This is a minimal Airflow DAG that runs a simple bash command.
    """

    bash_task = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'Hello from Airflow at logical date {{ ds }}'",
    )

    # In a more complex DAG, you would define dependencies like:
    # other_task >> bash_task

# Instantiate the DAG at the top level for Airflow to discover it.
minimal_dag = MinimalDag()
