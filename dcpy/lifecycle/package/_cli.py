from pathlib import Path

import typer
from tabulate import tabulate  # type: ignore

from dcpy.lifecycle import product_metadata
from dcpy.product_metadata import generate
from dcpy.product_metadata.writers.oti_xlsx.xlsx_writer import app as xlsx_writer_app

from .assemble import assemble_dataset_package
from .esri import app as esri_app
from .shapefiles import app as shapefile_app
from .validate import _validate

app = typer.Typer()
app.command(name="validate")(_validate)
app.add_typer(esri_app, name="esri")
app.add_typer(xlsx_writer_app, name="oti")
app.add_typer(shapefile_app, name="shapefile")

# Metadata commands
metadata_app = typer.Typer()


@metadata_app.command("generate")
def generate_metadata(
    dataset_paths: list[str] = typer.Argument(
        None,
        help="Dataset paths in format product.dataset. If not provided, generates for all datasets.",
    ),
    output_dir: Path = typer.Option(
        ..., "--output-dir", "-o", help="Output directory for generated files"
    ),
    version: str = typer.Option(
        ..., "--version", "-v", help="Product metadata version"
    ),
):
    """Generate metadata for datasets. If no datasets provided, generates for all."""
    org_md = product_metadata.load(version=version)

    # If no datasets provided, get all datasets
    if not dataset_paths:
        dataset_paths = []
        for product_name in org_md.metadata.products:
            try:
                product = org_md.product(product_name)
                for ds_id in product.metadata.datasets:
                    dataset_paths.append(f"{product_name}.{ds_id}")
            except Exception:
                pass

        typer.echo(
            f"No datasets specified, generating for all {len(dataset_paths)} datasets..."
        )

    results = generate.generate_metadata_assets(
        org_metadata=org_md,
        dataset_paths=dataset_paths,
        output_dir=output_dir,
    )

    # Display results table
    table_data = [
        [
            f"{r.product}.{r.dataset}",
            "✓" if r.success else "✗",
            ", ".join(r.files_generated) if r.files_generated else "-",
            r.error_message or "",
        ]
        for r in results
    ]

    table = tabulate(
        table_data,
        headers=["Dataset", "Success", "Files Generated", "Error"],
        tablefmt="simple",
    )
    typer.echo(table)

    # Raise exception if any failures
    failures = [r for r in results if not r.success]
    if failures:
        typer.echo(f"\n{len(failures)} dataset(s) failed", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"\nSuccessfully generated metadata for {len(results)} dataset(s)")


@metadata_app.command("list-versions")
def list_versions(
    version: str = typer.Option(
        ..., "--version", "-v", help="Product metadata version"
    ),
    output_file: Path = typer.Option(
        None, "--output", "-o", help="Output file (default: stdout)"
    ),
):
    """List all destination versions as JSON."""
    from dcpy.product_metadata.versions import DestinationVersions

    org_md = product_metadata.load(version=version)
    dest_versions = DestinationVersions.from_org_metadata(org_md)

    if output_file:
        dest_versions.to_json_file(output_file)
        typer.echo(
            f"Wrote {len(dest_versions.versions)} destination versions to {output_file}"
        )
    else:
        typer.echo(dest_versions.to_json_string())


@metadata_app.command("diff-versions")
def diff_versions(
    old_file: Path = typer.Argument(
        ..., help="Previous destination_versions.json file"
    ),
    new_file: Path = typer.Argument(..., help="Current destination_versions.json file"),
    output_file: Path = typer.Option(
        None, "--output", "-o", help="Output file (default: stdout)"
    ),
    json_format: bool = typer.Option(
        False, "--json", help="Output as JSON for machine reading"
    ),
):
    """Compare two destination version files and show changes."""
    from dcpy.product_metadata.versions import DestinationVersions

    old_versions = DestinationVersions.from_json_file(old_file)
    new_versions = DestinationVersions.from_json_file(new_file)

    diff = old_versions.compare(new_versions)

    if json_format:
        output = diff.model_dump_json(indent=2)
        if output_file:
            output_file.write_text(output + "\n")
            typer.echo(f"JSON diff written to {output_file}")
        else:
            typer.echo(output)
    else:
        output = diff.to_text()
        if output_file:
            output_file.write_text(output + "\n")
            typer.echo(f"Diff written to {output_file}")
        else:
            typer.echo(output)

    # Always print summary
    typer.echo(
        f"Summary: {diff.summary['added_count']} added, "
        f"{diff.summary['changed_count']} changed, "
        f"{diff.summary['removed_count']} removed"
    )


app.add_typer(metadata_app, name="metadata")


@app.command()
def assemble(
    dataset_key: str,
    version: str,
    source: str,
    path_override: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Output Path. Defaults to path configured in lifecycle.config",
    ),
    metadata_only: bool = typer.Option(
        False,
        "--metadata-only",
        "-m",
        help="Only Assemble Metadata. E.g. PDFs, Excel files, etc",
    ),
):
    product, *rest = dataset_key.split(".")
    dataset = rest[0] if rest else product  # e.g. lion.lion
    result = assemble_dataset_package(
        product=product,
        dataset=dataset,
        source_id=source,
        version=version,
        path_override=path_override,
        metadata_only=metadata_only,
    )

    if not result.success:
        error_msg = f"Encountered Errors assembling {dataset_key} from {source}: {result.result_summary} - {result.result_details}"
        typer.echo(error_msg)
        raise Exception(error_msg)
    else:
        typer.echo(result.package_path)
