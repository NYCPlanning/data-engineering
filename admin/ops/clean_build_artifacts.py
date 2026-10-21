import os
import subprocess

import requests
import typer

from dcpy.connectors.edm import connectors
from dcpy.lifecycle.builds import metadata
from dcpy.utils import postgres
from dcpy.utils.git import github
from dcpy.utils.logging import logger

BUILD_REPO = "data-engineering"

# Databases whose build schemas we never auto-drop (e.g. active development).
PRESERVED_DBS = [
    "db-cscl",  # we need to preserve schemas while this data product is in development
]
BUILD_DBS = [db for db in connectors.BUILD_DBS if db not in PRESERVED_DBS]

PROTECTED_BUILD_NAMES = ["nightly_qa"]


def get_active_build_names(as_schema=False) -> list[str]:
    branches = github.get_branches(repo=BUILD_REPO)  # all remote branches
    branch_build_names = sorted(
        PROTECTED_BUILD_NAMES
        + [metadata.schema_name(branch) if as_schema else branch for branch in branches]
    )
    return branch_build_names


def get_schema_bytes(pg_client: postgres.PostgresClient) -> dict[str, int]:
    """Size of each schema's tables and materialized views, with their indexes."""
    sizes = pg_client.execute_select_query(
        """
        SELECT
            n.nspname AS schema,
            sum(pg_total_relation_size(c.oid))::bigint AS bytes
        FROM pg_catalog.pg_class AS c
        INNER JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
        WHERE c.relkind IN ('r', 'm')
        GROUP BY n.nspname
        """
    )
    return dict(zip(sizes["schema"], sizes["bytes"]))


def delete_stale_schemas(active_build_names: list[str]) -> list[tuple[str, str, int]]:
    """Drops every build schema without an active build, and returns each dropped
    schema's database, name, and size in bytes."""
    logger.info(f"Potential active build schemas: {active_build_names}")
    active_build_test_names = [
        metadata.tests_schema_name(schema) for schema in active_build_names
    ]
    logger.info(f"Potential active build test schemas: {active_build_test_names}")
    active_schemas = active_build_names + active_build_test_names

    dropped = []
    for database in BUILD_DBS:
        pg_client_default = postgres.PostgresClient(
            schema=postgres.DEFAULT_POSTGRES_SCHEMA,
            database=database,
        )
        existing_build_schemas = pg_client_default.get_build_schemas()
        schema_bytes = get_schema_bytes(pg_client_default)

        for active_schema in active_schemas:
            if active_schema not in existing_build_schemas:
                logger.info(f"No build schema named {database}.{active_schema}")

        for build_schema in existing_build_schemas:
            if build_schema not in active_schemas:
                logger.warning(f"Dropping schema {database}.{build_schema}")
                pg_client_default.drop_schema(build_schema)
                dropped.append(
                    (database, build_schema, schema_bytes.get(build_schema, 0))
                )
            else:
                logger.info(f"Keeping schema {database}.{build_schema}")
    return dropped


def _size(num_bytes: float) -> str:
    for unit in ["B", "KB", "MB"]:
        if num_bytes < 1024:
            return f"{num_bytes:,.0f} {unit}"
        num_bytes /= 1024
    if num_bytes < 1024:
        return f"{num_bytes:,.1f} GB"
    return f"{num_bytes / 1024:,.1f} TB"


def write_job_summary(dropped: list[tuple[str, str, int]]) -> None:
    """Adds a table of dropped schemas to the GitHub Actions job summary."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    total = sum(num_bytes for _, _, num_bytes in dropped)
    lines = [
        "## Stale build schemas dropped",
        "",
        f"{len(dropped)} schemas, {_size(total)} cleared.",
        "",
        "| database | schema | size |",
        "|---|---|---:|",
        *[
            f"| {database} | {schema} | {_size(num_bytes)} |"
            for database, schema, num_bytes in sorted(dropped, key=lambda d: -d[2])
        ],
    ]
    with open(summary_path, "a") as f:
        f.write("\n".join(lines) + "\n")


def delete_stale_image_tags(active_build_names: list[str]) -> None:
    logger.info(f"Potential active branch build names: {active_build_names}")
    DOCKER_IMAGES = ["base", "dev", "build-base", "build-geosupport"]
    for image in DOCKER_IMAGES:
        tags = requests.get(
            f"https://hub.docker.com/v2/repositories/nycplanning/{image}/tags?page_size=1000"
        ).json()["results"]
        dev_tags: list[str] = [
            tag["name"] for tag in tags if tag["name"].startswith("dev-")
        ]
        for tag in dev_tags:
            if tag.removeprefix("dev-") not in active_build_names:
                logger.warning(f"Deleting tag {image}:{tag}")
                # The intonation seems a bit finicky so don't really want to implement in python rather than bash
                subprocess.call(["admin/ops/docker_delete.sh", image, tag])
            else:
                logger.info(f"Keeping tag {image}.{tag}")


app = typer.Typer(add_completion=False)


@app.command("schemas")
def _cli_wrapper_schemas():
    active_build_names = get_active_build_names(as_schema=True)
    dropped = delete_stale_schemas(active_build_names)
    write_job_summary(dropped)


@app.command("dockerhub_tags")
def _cli_wrapper_iamge_tags():
    active_build_names = get_active_build_names()
    delete_stale_image_tags(active_build_names)


if __name__ == "__main__":
    app()
