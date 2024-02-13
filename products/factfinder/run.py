from pathlib import Path
import typer

from dcpy.builds import load
from pipelines import acs_manual_update, decennial_manual_update

app = typer.Typer(add_completion=False)


@app.command()
def _run(
    dataset: str = typer.Argument(),
    upload: bool = typer.Option(
        False,
        "-u",
        "--upload",
        help="Upload after processing",
    ),
):
    assert dataset in [
        "acs",
        "decennial",
    ], "'acs' and 'decennial' only valid options for dataset."
    load_result = load.load_source_data(Path(f"{dataset}.yml"), keep_files=True)
    match dataset:
        case "acs":
            acs_manual_update.run(load_result, upload)

        case "decennial":
            decennial_manual_update.run(load_result, upload)


if __name__ == "__main__":
    app()
