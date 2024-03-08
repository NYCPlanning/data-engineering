import typer

from . import ingest, processing


def run(dataset: str, version: str | None = None):
    config = ingest.extract_and_archive_raw_dataset(dataset, version)

    ingest.transform_to_parquet(config)

    ## logic to apply transformations based on parsed config/template. Something like this
    # for step in config.processing.steps:
    #    func = getattr(processing, step)
    #    func(local_parquet_path)
    ##

    # recipes.archive(
    #     import_config, PARQUET_PATH
    # )  ## see comments in recipes for decisions to make about this function
    raise NotImplemented


app = typer.Typer(add_completion=False)


@app.command("archive_raw")
def _cli_wrapper_archive_raw_dataset(
    dataset: str = typer.Argument(),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Version of dataset being archived",
    ),
):
    ingest.extract_and_archive_raw_dataset(dataset, version)


@app.command("run")
def _cli_wrapper_run(
    dataset: str = typer.Argument(),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Version of dataset being archived",
    ),
):
    run(dataset, version)


if __name__ == "__main__":
    app()
