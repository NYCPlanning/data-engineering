import typer

from dcpy.configuration import INGEST_DEF_DIR
from dcpy.lifecycle.ingest.run import ingest


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
    push_to_s3: bool = typer.Option(False, "--push-to-s3", "-s"),
    csv: bool = typer.Option(
        False, "-c", "--csv", help="Output csv locally as well as parquet"
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Overwrite the archived version instead of failing. Requires --version",
    ),
):
    if INGEST_DEF_DIR is None:
        raise KeyError("Missing required env variable: 'TEMPLATE_DIR'")

    # This entrypoint takes any dataset, and the recipes bucket isn't versioned, so
    # naming the version is what keeps an overwrite from quietly eating whichever one
    # happened to be derived.
    if overwrite and not version:
        raise ValueError(
            "--overwrite requires an explicit --version, so replacing archived data is deliberate."
        )

    ingest(
        dataset_id,
        version,
        mode=mode,
        latest=latest,
        push=push_to_s3,
        output_csv=csv,
        overwrite_okay=overwrite,
    )
