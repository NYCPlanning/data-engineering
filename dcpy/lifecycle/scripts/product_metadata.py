from pathlib import Path
from tabulate import tabulate  # type: ignore
import typer

from dcpy.lifecycle import product_metadata

app = typer.Typer()


@app.command("validate_repo")
def validate_repo_cli(repo_path: Path):
    errors = product_metadata.load(
        org_md_path_override=repo_path, version="TEST_VERSION"
    ).validate_metadata()
    if errors:
        flattened_errors = []
        for p_name, ds_errs_dict in errors.items():
            for ds_name, ds_errs_list in ds_errs_dict.items():
                flattened_errors += [[p_name, ds_name, e] for e in ds_errs_list]
        raise Exception(
            tabulate(
                flattened_errors,
                headers=["product", "dataset", "error"],
                tablefmt="presto",
                maxcolwidths=[10, 10, 50],
            )
        )
