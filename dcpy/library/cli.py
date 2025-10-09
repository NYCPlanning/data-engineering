import json
import os
from rich import box
from rich.console import Console
from rich.table import Table
import typer

from dcpy.models.library import Config
from . import aws_s3_bucket
from .archive import Archive
from .s3 import S3

console = Console()
app = typer.Typer()
s3 = S3()


# fmt: off
@app.command()
def archive(
    path: str = typer.Option(None, "--path", "-f", help="Path to config yml"),
    output_formats: list[str] = typer.Option(["pgdump", "parquet", "csv"], "--output-format", "-o", help="csv, geojson, shapefile, pgdump and parquet"),
    push: bool = typer.Option(False, "--s3", "-s", help="Push to s3"),
    clean: bool = typer.Option(False, "--clean", "-c", help="Remove temporary files"),
    latest: bool = typer.Option(False, "--latest", "-l", help="Tag with latest"),
    name: str = typer.Option(None, "--name", "-n", help="Name of the dataset, if supplied, \"path\" will ignored"),
    compress: bool = typer.Option(False, "--compress", help="Compress output"),
    inplace: bool = typer.Option(False, "--inplace", help="Only keeping zipped file"),
    postgres_url: str = typer.Option(None, "--postgres-url", help="Postgres connection url"),
    version: str = typer.Option(None, "--version", "-v", help="Custom version input"),
    source_path_override: str = typer.Option(None, "--source-path-override", help="Overrides path source if applicable")
) -> None:
# fmt: on
    """
    Archive a dataset from source to destination
    """
    if not name and not path:
        message = typer.style("\nPlease specify dataset NAME or PATH to configuration\n", fg=typer.colors.RED)
        typer.echo(message)
    else:
        a = Archive()
        config: Config | None = None
        for output_format in output_formats:
            config = a(
                path=path,
                output_format=output_format,
                push=push,
                clean=clean,
                latest=latest,
                name=name,
                compress=compress,
                inplace=inplace,
                postgres_url=postgres_url,
                version=version,
                source_path_override=source_path_override,
            )
        assert config

# fmt: off
@app.command()
def delete(
    name: str = typer.Option(None, "--name", "-n", help="Name of the dataset to remove"),
    version: str = typer.Option(None, "--version", "-v", help="Version of dataset to remove. \
        If not specified, all versions of a particular dataset will be deleted"),
    extension: str = typer.Option(None, "--extension", "-e", help="Extension of file to remove. \
        If not specified, all files of a particular version will be deleted"),
    key: str = typer.Option(None, "--key", "-k", help="Full key of dataset to remove. \
        If provided, will take precedent over the other 3 optional arguments"),
) -> None:
# fmt: on
    """
    Delete a file from s3 library
    """
    if key:
        typer.echo(f"Deleting: {key}")
        s3.rm(key)
    elif name:
        keys = s3.ls(f"datasets/{name}/")
        if version:
            keys = [k for k in keys if (k.split("/")[2] == version)]
            if extension:
                keys = [k for k in keys if (k.split(".")[1] == extension)]
        message = "\n\t".join(keys)
        typer.echo(f"Deleting:\n\t{message}")
        s3.rm(*keys)

# fmt: off
@app.command()
def show(
    name: str,
    detail: bool = typer.Option(False, "--detail", help="detailed info including key, version and last modified"),
    output_json: bool = typer.Option(False, "--json", help="output info in json format"),
) -> None:
# fmt: on
    """
    Display files available in the s3 library for a given dataset.
    Options to display in table format for json format
    """
    _keys = s3.ls(f"datasets/{name}/")
    _info = []
    for key in _keys:
        info = s3.info(key)
        info.pop('ResponseMetadata')
        info['Key'] = key
        info['Url'] = f'{os.environ["AWS_S3_ENDPOINT"]}/{aws_s3_bucket}/{key}'
        info['LastModified'] = info['LastModified'].isoformat()[:10]
        _info.append(info)

    if detail:
        if output_json:
            console.print(json.dumps(_info, indent=4, default=str))
        else:
            table = Table(show_header=True, header_style="bold magenta1", box=box.SIMPLE)
            table.add_column("Key", justify="left", style="bold blue")
            table.add_column("Version", justify="right", style="green")
            table.add_column("LastModified", justify="right", style="green")
            table.add_column("Url", justify="left", style="green")
            for info in _info:
                table.add_row(
                    '/'.join(info['Key'].split('/')[-2:]),
                    info['Metadata']['version'],
                    info['LastModified'],
                    info['Url'],
                )
            console.print(table)

    if not detail:
        versions = list(set([info['Metadata']['version'] for info in _info]))
        latest_version = [info['Metadata']['version'] for info in _info if 'latest/config.json' in info['Key']][0]
        output = {
            'versions': versions,
            'latest': latest_version
        }
        if output_json:
            console.print(json.dumps(output, indent=4, default=str))
        else:
            for version in versions:
                message = version
                if version == latest_version:
                    message = f'{version} (latest)'
                console.print(message)

def run() -> None:
    """Run commands."""
    app()


if __name__ == "__main__":
    run()
