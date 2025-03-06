from pathlib import Path
import typer

from .run import ingest
from dcpy.configuration import TEMPLATE_DIR

app = typer.Typer(add_completion=False)


@app.command("ingest")
def _cli_wrapper_run(
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
    local_file_path: Path = typer.Option(
        None,
        "--local-file-path",
        "-p",
        help="Use local file path as source, overriding source in template",
    ),
    template_dir: Path = typer.Option(
        TEMPLATE_DIR,
        "--template-dir",
        "-t",
        help="Local path to folder with templates. Overrides `TEMPLATE_DIR` env variable.",
    ),
):
    ingest(
        dataset_id,
        version,
        mode=mode,
        latest=latest,
        skip_archival=skip_archival,
        output_csv=csv,
        local_file_path=local_file_path,
        template_dir=template_dir,
    )
