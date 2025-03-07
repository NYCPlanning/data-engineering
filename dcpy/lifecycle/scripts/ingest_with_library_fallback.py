import typer

from dcpy.lifecycle.ingest import ingest
from dcpy.configuration import TEMPLATE_DIR


def run(
    dataset_id: str = typer.Argument(),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Version of dataset being archived",
    ),
    mode: str = typer.Option(None, "-m", "--mode", help="Preprocessing mode"),
    latest: bool = typer.Option(
        False, "-l", "--latest", help="Push to latest folder in s3"
    ),
    skip_archival: bool = typer.Option(False, "--skip-archival", "-s"),
    csv: bool = typer.Option(
        False, "-c", "--csv", help="Output csv locally as well as parquet"
    ),
):
    if TEMPLATE_DIR is None:
        raise KeyError("Missing required env variable: 'TEMPLATE_DIR'")

    if (TEMPLATE_DIR / f"{dataset_id}.yml").exists():
        ingest(
            dataset_id,
            version,
            mode=mode,
            latest=latest,
            skip_archival=skip_archival,
            output_csv=csv,
        )
    else:
        from dcpy.library import cli as library_cli

        library_cli.archive(
            name=dataset_id,
            version=version,
            latest=latest,
            push=not skip_archival,
            # defaults - needed since typer defaults are odd when called in python rather than cli
            output_formats=["pgdump", "parquet", "csv"],
            path=None,  # type: ignore
            clean=False,
            compress=False,
            inplace=False,
            postgres_url=None,  # type: ignore
            source_path_override=None,  # type: ignore
        )
