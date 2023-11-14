from dcpy.utils import postgres
from dcpy.utils.logging import logger
from dcpy.connectors import github
from dcpy.connectors.edm import build_metadata

from . import BUILD_REPO, BUILD_DBS


def get_active_build_names() -> list:
    # use the hard-coded "build_" prefix from github action workflow files
    # and the event-based prefixes from build_metadata.build_name
    branches = github.get_branches(repo=BUILD_REPO)  # all remote branches
    branch_build_names = sorted(
        [
            build_metadata.build_name(event="branch", source=branch)
            for branch in branches
        ]
        + ["build_" + branch for branch in branches]
    )
    logger.info(f"Potential active branch build names: {branch_build_names}")

    pull_requests = github.get_pull_requests(repo=BUILD_REPO)  # all open PR numbers
    pr_build_names = sorted(
        [
            build_metadata.build_name(event="pull_request", source=pr)
            for pr in pull_requests
        ]
        + ["build_" + pr for pr in pull_requests]
    )
    logger.info(f"Potential active PR build names: {pr_build_names}")

    return pr_build_names + branch_build_names


def delete_stale_builds(active_build_names: list):
    for database in BUILD_DBS:
        pg_client_default = postgres.PostgresClient(
            schema=postgres.DEFAULT_POSTGRES_SCHEMA,
            database=database,
        )
        existing_build_schemas = pg_client_default.get_build_schemas()

        for active_name in active_build_names:
            if active_name not in existing_build_schemas:
                logger.info(f"No build schema named {database}.{active_name}")

        for build_schema in existing_build_schemas:
            if build_schema not in active_build_names:
                logger.warning(f"Dropping schema {database}.{build_schema}")
                pg_client_default.drop_schema(build_schema)
            else:
                logger.info(f"Keeping schema {database}.{build_schema}")


if __name__ == "__main__":
    active_build_names = get_active_build_names()
    delete_stale_builds(active_build_names)
