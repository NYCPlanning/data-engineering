from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.results import RunExecutionResult

from build_scripts import PG_CLIENT, SQL_QUERY_DIR
from dcpy.utils import postgres
from dcpy.utils.logging import logger


def _execute_sql_script(step_query=str):
    logger.info(f"Running transform step {step_query} ...")
    postgres.execute_file_via_shell(
        PG_CLIENT.engine_uri,
        SQL_QUERY_DIR / f"{step_query}.sql",
    )


def _invoke_dbt(run_args=list[str]):
    dbt_cli_call = f"dbt {' '.join(run_args)}"
    logger.info(f"Running '{dbt_cli_call}' ...")
    dbt_runner = dbtRunner()
    runner_result: dbtRunnerResult = dbt_runner.invoke(run_args)

    # dbtRunnerResult attributes https://docs.getdbt.com/reference/programmatic-invocations#dbtrunnerresult
    if runner_result.exception:
        raise RuntimeError(runner_result.exception)

    match runner_result.result:
        case RunExecutionResult():
            for r in runner_result.result:
                logger.info(f"{r.node.name}: {r.status}")

    if not runner_result.success:
        logger.error(f"dbt error with args: {dbt_cli_call}")
        raise RuntimeError(
            "dbt invocation completed with at least one handled error (e.g. test failure, model build error)"
        )


def staging():
    _invoke_dbt(["test", "--select", "source:*"])
    _execute_sql_script("staging")


def intermediate():
    _execute_sql_script("libraries")
    _execute_sql_script("green_spaces")
    _execute_sql_script("historic_landmarks")
    _invoke_dbt(["test", "--select", "libraries green_spaces historic_landmarks"])


def product():
    _execute_sql_script("product/templatedb")
    _execute_sql_script("product/aggregation/templatedb_boroughs")
    _invoke_dbt(["test", "--select", "product.templatedb"])
    _invoke_dbt(["test", "--select", "product.aggregation"])


if __name__ == "__main__":
    _invoke_dbt(["deps"])
    _invoke_dbt(["debug"])
    staging()
    intermediate()
    product()
