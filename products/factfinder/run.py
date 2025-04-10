import typer

from dcpy.lifecycle.builds import load, plan
from .pipelines import acs_manual_update, decennial_manual_update
from .paths import ROOT_PATH

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
):
    assert dataset in [
        "acs",
        "decennial",
    ], "'acs' and 'decennial' only valid options for dataset."
    lockfile = plan.plan(ROOT_PATH / f"{dataset}.yml")
    load_result = load.load_source_data_from_resolved_recipe(lockfile)
    match dataset:
        case "acs":
            acs_manual_update.build(vesion, load_result)

        case "decennial":
            decennial_manual_update.build(vesion, load_result)


if __name__ == "__main__":
    app()
