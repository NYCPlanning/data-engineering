from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.results import RunExecutionResult

from dcpy.utils import postgres
from dcpy.utils.logging import logger
from build_scripts import SQL_QUERY_DIR, PG_CLIENT


def _run_step(step_query=str):
    logger.info(f"Running transform step {step_query} ...")
    postgres.execute_file_via_shell(
        PG_CLIENT.engine_uri,
        SQL_QUERY_DIR / f"{step_query}.sql",
    )


def _run_dbt_step(run_args=list[str]):
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
    _run_step("staging")


def intermediate():
    _run_step("libraries")
    _run_step("green_spaces")
    _run_step("historic_landmarks")


def product():
    _run_step("templatedb")
    _run_step("aggregation/templatedb_boroughs")


def test():
    _run_dbt_step(["deps"])
    _run_dbt_step(["debug"])
    _run_dbt_step(["test"])


if __name__ == "__main__":
    staging()
    intermediate()
    product()
    test()
