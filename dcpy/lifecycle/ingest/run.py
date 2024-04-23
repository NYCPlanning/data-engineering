import typer

from dcpy.connectors.edm import recipes
from . import configure, extract, transform, TMP_DIR


def run(dataset: str, version: str | None = None):
    config = configure.get_config(dataset, version)

    # download dataset
    extract.download_file_from_source(
        config.source, config.raw_filename, config.version
    )
    file_path = TMP_DIR / config.raw_filename

    # archive to edm-recipes/raw_datasets
    recipes.archive_raw_dataset(config, file_path)

    # convert raw data to parquet format
    transform.to_parquet(config.file_format, file_path)

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
