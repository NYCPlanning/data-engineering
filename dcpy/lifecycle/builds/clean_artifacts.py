import requests
import subprocess
import typer

from dcpy.utils import postgres
from dcpy.utils.logging import logger
from dcpy.connectors import github
from dcpy.lifecycle.builds import metadata

from . import BUILD_REPO, BUILD_DBS


def get_active_build_names(as_schema=False) -> list[str]:
    branches = github.get_branches(repo=BUILD_REPO)  # all remote branches
    branch_build_names = sorted(
        [
            metadata.build_name(name=branch) if as_schema else branch
            for branch in branches
        ]
    )
    return branch_build_names


def delete_stale_schemas(active_build_names: list[str]):
    logger.info(f"Potential active branch build names: {active_build_names}")
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
            build_test_schema = metadata.build_tests_name(build_name=build_schema)
            if build_schema not in active_build_names:
                logger.warning(f"Dropping schema {database}.{build_schema}")
                pg_client_default.drop_schema(build_schema)
                logger.warning(f"Dropping tests schema {database}.{build_test_schema}")
                pg_client_default.drop_schema(build_test_schema)
            else:
                logger.info(f"Keeping schema {database}.{build_schema}")
                logger.info(f"Keeping tests schema {database}.{build_test_schema}")


def delete_stale_image_tags(active_build_names: list[str]) -> None:
    logger.info(f"Potential active branch build names: {active_build_names}")
    DOCKER_IMAGES = ["dev", "build-base", "build-geosupport"]
    for image in DOCKER_IMAGES:
        tags: list[str] = requests.get(
            f"https://hub.docker.com/v2/repositories/nycplanning/{image}/tags?page_size=1000"
        ).json()["results"]
        dev_tags = [tag for tag in tags if tag["name"].startswith("dev-")]
        for tag in dev_tags:
            if tag.removeprefix("dev-") not in active_build_names:
                logger.warning(f"Deleting tag {image}:{tag['name']}")
                # Should we include this file in dcpy? A little odd to rely on this file existing, but it's going to be a bit hacky regardless
                # The intonation seems a bit finicky so don't really want to implement in python rather than bash
                subprocess.call(["docker/delete.sh", image, tag["name"]])
            else:
                logger.info(f"Keeping tag {image}.{tag['name']}")


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
