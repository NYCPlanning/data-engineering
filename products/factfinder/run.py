from pathlib import Path
from typing import Optional

import typer

from dcpy.lifecycle.builds import load, plan

from .paths import ROOT_PATH
from .pipelines import acs_manual_update, decennial_manual_update

app = typer.Typer(add_completion=False)


@app.command()
def _run(
    dataset: str = typer.Argument(),
    vesion: str = typer.Argument(),
    upload: bool = typer.Option(
        False,
        "-u",
        "--upload",
        help="Upload after processing",
    ),
    metadata_only: bool = typer.Option(
        False,
        "--metadata-only",
        help="Only process metadata (requires --metadata-file)",
    ),
    metadata_file: Optional[Path] = typer.Option(
        None,
        "--metadata-file",
        help="Path to the Excel metadata file (required with --metadata-only)",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
):
    assert dataset in [
        "acs",
        "decennial",
    ], "'acs' and 'decennial' only valid options for dataset."

    if metadata_only:
        assert metadata_file is not None, (
            "--metadata-file is required with --metadata-only"
        )
        match dataset:
            case "acs":
                acs_manual_update.run_metadata(metadata_file)
            case "decennial":
                raise typer.BadParameter(
                    "--metadata-only is not supported for decennial"
                )
        return

    lockfile = plan.plan(ROOT_PATH / f"{dataset}.yml")
    load_result = load.load_source_data_from_resolved_recipe(lockfile)
    match dataset:
        case "acs":
            acs_manual_update.build(vesion, load_result)

        case "decennial":
            decennial_manual_update.build(vesion, load_result)


if __name__ == "__main__":
    app()
