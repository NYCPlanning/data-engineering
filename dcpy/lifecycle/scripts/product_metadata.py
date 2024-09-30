from pathlib import Path
import typer

from dcpy.models.product import metadata as product_metadata


def validate_repo(repo_path: Path):
    return product_metadata.Repo.from_path(repo_path).validate_metadata()


app = typer.Typer()


@app.command("validate_repo")
def validate_repo_cli(repo_path: Path):
    [print(f"{e}\n") for e in validate_repo(repo_path).items()]
