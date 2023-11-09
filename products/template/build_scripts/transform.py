from dcpy.utils import postgres
from dcpy.utils.logging import logger
from build_scripts import SQL_QUERY_DIR, PG_CLIENT

from dbt.cli.main import dbtRunner, dbtRunnerResult


def _run_step(step_query=str):
    logger.info(f"Running transform script {step_query}")
    postgres.execute_file_via_shell(
        PG_CLIENT.engine_uri,
        SQL_QUERY_DIR / f"{step_query}.sql",
    )


def _run_dbt_step(run_args=list[str]):
    logger.info(f"Running dbt step via '{' '.join(run_args)}'")
    res: dbtRunnerResult = dbtRunner().invoke(run_args)
    for r in res.result:
        logger.info(f"{r.node.name}: {r.status}")


def staging_dbt():
    dbtRunner().invoke(["debug"])
    _run_dbt_step(["seed"])
    _run_dbt_step(["run", "--select", "staging"])


def staging():
    _run_step("staging")


def intermediate():
    _run_step("libraries")
    _run_step("green_spaces")
    _run_step("historic_landmarks")


def product():
    _run_step("templatedb")
    _run_step("aggregation/boroughs")


if __name__ == "__main__":
    staging_dbt()
    staging()
    intermediate()
    product()
