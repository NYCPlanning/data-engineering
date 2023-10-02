from dcpy.utils import postgres
from dcpy.utils.logging import logger
from build_scripts import SQL_QUERY_DIR, PG_CLIENT


def _run_step(step_query=str):
    logger.info(f"Running transform script {step_query}")
    postgres.execute_file_via_shell(
        PG_CLIENT.engine_uri,
        SQL_QUERY_DIR / f"{step_query}.sql",
    )


def staging():
    _run_step("staging")


def intermediate():
    _run_step("libraries")
    _run_step("green_spaces")
    _run_step("historic_landmarks")


def outputs():
    _run_step("templatedb")
    _run_step("aggregation/boroughs")


if __name__ == "__main__":
    staging()
    intermediate()
    outputs()
