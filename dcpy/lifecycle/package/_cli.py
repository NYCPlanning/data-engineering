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
    """List all destination versions."""
    org_md = product_metadata.load(version=version)
    versions = org_md.get_all_destination_current_versions()

    output = "\n".join(versions)

    if output_file:
        output_file.write_text(output + "\n")
        typer.echo(f"Wrote {len(versions)} destination versions to {output_file}")
    else:
        typer.echo(output)


@metadata_app.command("diff-versions")
def diff_versions(
    old_file: Path = typer.Argument(..., help="Previous destination_versions.txt file"),
    new_file: Path = typer.Argument(..., help="Current destination_versions.txt file"),
    output_file: Path = typer.Option(
        None, "--output", "-o", help="Output file (default: stdout)"
    ),
    json_format: bool = typer.Option(
        False, "--json", help="Output as JSON for machine reading"
    ),
):
    """Compare two destination version files and show changes."""
    import json

    # Read versions
    prev_versions = {}
    for line in old_file.read_text().strip().split("\n"):
        if "|" in line:
            dest, ver = line.split("|", 1)
            prev_versions[dest] = ver

    curr_versions = {}
    for line in new_file.read_text().strip().split("\n"):
        if "|" in line:
            dest, ver = line.split("|", 1)
            curr_versions[dest] = ver

    # Categorize changes
    added = []
    changed = []
    removed = []

    for dest, ver in sorted(curr_versions.items()):
        if dest not in prev_versions:
            parts = dest.split(".")
            added.append(
                {
                    "destination": dest,
                    "product": parts[0] if len(parts) >= 1 else "",
                    "dataset": parts[1] if len(parts) >= 2 else "",
                    "destination_id": parts[2] if len(parts) >= 3 else "",
                    "version": ver,
                }
            )
        elif prev_versions[dest] != ver:
            parts = dest.split(".")
            changed.append(
                {
                    "destination": dest,
                    "product": parts[0] if len(parts) >= 1 else "",
                    "dataset": parts[1] if len(parts) >= 2 else "",
                    "destination_id": parts[2] if len(parts) >= 3 else "",
                    "old_version": prev_versions[dest],
                    "new_version": ver,
                }
            )

    for dest, ver in sorted(prev_versions.items()):
        if dest not in curr_versions:
            parts = dest.split(".")
            removed.append(
                {
                    "destination": dest,
                    "product": parts[0] if len(parts) >= 1 else "",
                    "dataset": parts[1] if len(parts) >= 2 else "",
                    "destination_id": parts[2] if len(parts) >= 3 else "",
                    "version": ver,
                }
            )

    if json_format:
        # JSON output for machine reading
        output_data = {
            "added": added,
            "changed": changed,
            "removed": removed,
            "summary": {
                "added_count": len(added),
                "changed_count": len(changed),
                "removed_count": len(removed),
            },
        }
        output = json.dumps(output_data, indent=2)

        if output_file:
            output_file.write_text(output + "\n")
            typer.echo(f"JSON diff written to {output_file}")
            typer.echo(
                f"Summary: {len(added)} added, {len(changed)} changed, {len(removed)} removed"
            )
        else:
            typer.echo(output)
    else:
        # Human-readable text output
        output_lines = []

        if added:
            output_lines.append("## Added")
            output_lines.extend([f"+ {a['destination']}|{a['version']}" for a in added])
            output_lines.append("")

        if changed:
            output_lines.append("## Changed")
            output_lines.extend(
                [
                    f"  {c['destination']}|{c['old_version']} -> {c['new_version']}"
                    for c in changed
                ]
            )
            output_lines.append("")

        if removed:
            output_lines.append("## Removed")
            output_lines.extend(
                [f"- {r['destination']}|{r['version']}" for r in removed]
            )
            output_lines.append("")

        if not added and not changed and not removed:
            output_lines.append("No changes detected.")

        output = "\n".join(output_lines)

        if output_file:
            output_file.write_text(output + "\n")
            typer.echo(f"Diff written to {output_file}")
            typer.echo(
                f"Summary: {len(added)} added, {len(changed)} changed, {len(removed)} removed"
            )
        else:
            typer.echo(output)


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
