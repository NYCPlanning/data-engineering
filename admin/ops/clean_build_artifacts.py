import subprocess

import requests
import typer

from dcpy.lifecycle.builds import BUILD_DBS, BUILD_REPO, metadata
from dcpy.utils import postgres
from dcpy.utils.git import github
from dcpy.utils.logging import logger

PROTECTED_BUILD_NAMES = ["nightly_qa"]


def get_active_build_names(as_schema=False) -> list[str]:
    branches = github.get_branches(repo=BUILD_REPO)  # all remote branches
    branch_build_names = sorted(
        PROTECTED_BUILD_NAMES
        + [
            metadata.build_name(name=branch) if as_schema else branch
            for branch in branches
        ]
    )
    return branch_build_names


def delete_stale_schemas(active_build_names: list[str]):
    logger.info(f"Potential active build schemas: {active_build_names}")
    active_build_test_names = [
        metadata.build_tests_name(build_name=schema) for schema in active_build_names
    ]
    logger.info(f"Potential active build test schemas: {active_build_test_names}")
    active_schemas = active_build_names + active_build_test_names

    for database in BUILD_DBS:
        pg_client_default = postgres.PostgresClient(
            schema=postgres.DEFAULT_POSTGRES_SCHEMA,
            database=database,
        )
        existing_build_schemas = pg_client_default.get_build_schemas()

        for active_schema in active_schemas:
            if active_schema not in existing_build_schemas:
                logger.info(f"No build schema named {database}.{active_schema}")

        for build_schema in existing_build_schemas:
            if build_schema not in active_schemas:
                logger.warning(f"Dropping schema {database}.{build_schema}")
                pg_client_default.drop_schema(build_schema)
            else:
                logger.info(f"Keeping schema {database}.{build_schema}")


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
    delete_stale_schemas(active_build_names)


@app.command("dockerhub_tags")
def _cli_wrapper_iamge_tags():
    active_build_names = get_active_build_names()
    delete_stale_image_tags(active_build_names)


if __name__ == "__main__":
    app()
