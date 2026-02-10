from pathlib import Path

import typer
from ruamel.yaml import YAML

from dcpy.lifecycle.builds import plan
from dcpy.models.lifecycle.builds import InputDataset, Recipe

DEFAULT_RECIPE = "recipe.yml"

app = typer.Typer(add_completion=False)

yaml = YAML()


class RecipeEditor:
    """
    Context manager class that allows recipe files to be edited (and validated)
    in a with clause. Validation occurs at time of reading and writing.
    """

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        plan.recipe_from_yaml(self.path)
        with self.path.open() as f:
            self.recipe = yaml.load(f)
        return self.recipe

    def __exit__(self, *args):
        Recipe(**self.recipe)
        with self.path.open("w") as f:
            yaml.dump(self.recipe, f)


def generate_sources_yml(project_dir: Path, recipe_path: Path = Path(DEFAULT_RECIPE)):
    """Generate _source.yml from recipe.yml"""
    raise NotImplementedError()


def add_to_recipe_sources(dataset, source):
    """Small helper to append dataset to yml from _sources.yml"""
    source["tables"].append({"name": dataset})
    return source


def add_dbt_source(
    dataset: str,
    project_dir: Path,
    recipe_path: Path = Path(DEFAULT_RECIPE),
    add_staging_stubs: bool = False,
):
    """
    Given dataset name, adds a source dataset to a recipe and _sources.yml
    Optionally creates a stub sql file in staging folder and adds source to _staging_models.yml

    TODO this function is currently untested, and if we start to use operationally, or if
    additional functionality begins to use it outside of CLI calls, it certainly needs tests
    """
    with RecipeEditor(project_dir / recipe_path) as recipe:
        recipe["inputs"]["datasets"].append(
            InputDataset(id=dataset).model_dump(
                exclude_defaults=True, exclude_none=True, mode="json"
            )
        )

    sources_path = project_dir / "models" / "_sources.yml"
    with sources_path.open() as f:
        sources = yaml.load(f)
    sources["sources"] = [
        add_to_recipe_sources(dataset, s) if s["name"] == "recipe_sources" else s
        for s in sources["sources"]
    ]
    with sources_path.open("w") as f:
        yaml.dump(sources, f)

    if add_staging_stubs:
        staging_yml = project_dir / "models" / "staging" / "_staging_models.yml"
        stg_name = f"stg__{dataset}"
        with staging_yml.open() as f:
            staging = yaml.load(f)
        staging["models"].append({"name": stg_name})
        with staging_yml.open("w") as f:
            yaml.dump(staging, f)
        (project_dir / "models" / "staging" / f"{stg_name}.sql").touch()


@app.command("add_source")
def _cli_wrapper_add_source(
    dataset: str = typer.Argument(),
    project_dir: Path = typer.Option(Path("."), "--project-dir", "-p"),
    recipe_path: Path = typer.Option(
        Path(DEFAULT_RECIPE),
        "--recipe-path",
        "-r",
        help="Path of recipe file to use, relative to project directory",
    ),
    add_staging_stubs: bool = typer.Option(
        False,
        "--add-staging-stubs",
        "--stg",
        help="add stub model to staging, both sql file and entry in _staging_models.yml",
    ),
):
    add_dbt_source(dataset, project_dir, recipe_path, add_staging_stubs)


if __name__ == "__main__":
    app()
