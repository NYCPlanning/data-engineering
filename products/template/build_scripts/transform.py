from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.results import RunExecutionResult

from dcpy.utils.logging import logger


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


if __name__ == "__main__":
    _invoke_dbt(["deps"])
    _invoke_dbt(["debug"])
    _invoke_dbt(["seed"])
    _invoke_dbt(["test", "--select", "source:*"])
    _invoke_dbt(["build", "--select", "staging"])
    _invoke_dbt(["build", "--select", "intermediate"])
    _invoke_dbt(["build", "--select", "product"])
