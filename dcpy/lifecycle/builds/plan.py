import csv
import os
from pathlib import Path

import pandas as pd
import typer
import yaml
from jinja2 import (
    Environment,
    StrictUndefined,
    TemplateSyntaxError,
    Undefined,
    UndefinedError,
    meta,
)

from dcpy.configuration import BUILD_NAME
from dcpy.connectors.edm import recipes
from dcpy.connectors.edm.models import BuildKey, DatasetType, DraftKey, PublishKey
from dcpy.lifecycle.builds import config, utils
from dcpy.lifecycle.builds.connector import (
    get_published_default_connector,
    get_recipes_default_connector,
)
from dcpy.lifecycle.builds.models import (
    InputDatasetDefaults,
    Recipe,
    RecipeInputsVersionStrategy,
)
from dcpy.lifecycle.connector_registry import connectors
from dcpy.utils import versions
from dcpy.utils.logging import logger

DEFAULT_RECIPE = "recipe.yml"
RECIPE_FILE_TYPE_PREFERENCE = [
    DatasetType.pg_dump,
    DatasetType.parquet,
    DatasetType.csv,
]

ARTIFACTS = [
    "recipe.lock.yml",
    "build_metadata.json",
    "source_data_versions.csv",
]


def get_recipe_template_variables(recipe_path: Path) -> list[str]:
    """Extract Jinja2 template variables from a recipe file.

    Uses Jinja2's meta.find_undeclared_variables to parse the recipe
    and find all template variables that need to be provided.

    Args:
        recipe_path: Path to the recipe.yml file

    Returns:
        Sorted list of template variable names found in the recipe

    Example:
        >>> vars = get_recipe_template_variables(Path("recipe.yml"))
        >>> # Returns: ["BUILD_ENGINE_SCHEMA", "BUILD_ENV_BRANCH"]
    """
    with open(recipe_path, "r") as f:
        recipe_text = f.read()

    env = Environment()
    parsed_content = env.parse(recipe_text)
    jinja_vars = meta.find_undeclared_variables(parsed_content)

    return sorted(jinja_vars)


class PreserveUndefined(Undefined):
    """Jinja2 Undefined that preserves template syntax as YAML-safe strings.

    This allows parsing recipes with Jinja2 templates without rendering them.
    Templates like {{ VAR }} are preserved as strings in the parsed recipe.
    """

    def __str__(self):
        # Return the template as a single-quoted string so YAML parses it as a string literal
        # This prevents YAML from trying to interpret {{ }} as a flow mapping
        return f"'{{{{ {self._undefined_name} }}}}'"


def resolve_version(recipe: Recipe) -> str:
    match recipe.version_strategy:
        case None:
            raise Exception("No version or version_strategy provided")
        case versions.SimpleVersionStrategy.today:
            return versions.generate_today().label
        case versions.SimpleVersionStrategy.first_of_month:
            return versions.generate_first_of_month().label
        case versions.SimpleVersionStrategy.bump_latest_release:
            previous_version = get_published_default_connector().get_latest_version(
                recipe.product
            )
            assert previous_version is not None
            return versions.bump(
                previous_version=previous_version,
                bump_type=recipe.version_type,
            ).label
        case versions.BumpLatestRelease() as bump:
            previous_version = get_published_default_connector().get_latest_version(
                recipe.product
            )
            assert previous_version is not None
            return versions.bump(
                previous_version=previous_version,
                bump_type=recipe.version_type,
                bump_by=bump.bump_latest_release,
            ).label
        case versions.PinToSourceDataset():
            dataset = recipe.version_strategy.pin_to_source_dataset
            inputs_by_name = {d.id: d for d in recipe.inputs.datasets}
            if dataset not in inputs_by_name:
                raise ValueError(
                    f"Cannot pin build version to dataset '{dataset}' as it is not an input dataset"
                )
            input = inputs_by_name[dataset]
            if not input.version and (
                input.version_env_var
                or (
                    recipe.inputs.missing_versions_strategy
                    != RecipeInputsVersionStrategy.find_latest
                )
            ):
                raise ValueError(
                    "To use 'pin to source dataset' version strategy, source input dataset must either be latest or explicit version"
                )
            return input.version or get_recipes_default_connector().get_latest_version(
                dataset
            )


def plan_recipe(
    recipe_path: Path,
    version: str | None = None,
    vars: dict[str, str] | None = None,
    build_name: str | None = None,
    branch: str | None = None,
    env_vars: dict[str, str] | None = None,
) -> Recipe:
    """Plan recipe versions and file types for a product.

    Similar to pip freeze, determines recipe versions and file types to use for a build.
    A base_recipe may be specified, in which case it's important to note that
    the missing versions strategy will be applied AFTER the recipe inputs are
    merged with the base.

    Args:
        recipe_path: Path to the recipe.yml file
        version: Optional version to use for the build
        vars: Optional dict of template variables to use instead of BUILD_ENV_* environment
              variables. All vars or none approach - if provided, environment variables are
              not used for templating.
        build_name: Optional build name/identifier (e.g., branch name, partition key).
                   If not provided, falls back to BUILD_NAME environment variable.
        branch: Optional branch name (e.g., "main", "fix-bug").
               If provided, sets recipe.branch.
        env_vars: Optional dict of environment variables to add to recipe.env
                 (e.g., BUILD_ENGINE_SCHEMA, BUILD_ENGINE_DB).
    """
    recipe: Recipe = recipe_from_yaml(recipe_path, vars=vars)

    # Determine the recipe version
    if version:
        recipe.version = version
    elif recipe.version is None:
        recipe.version = resolve_version(recipe)

    # Add computed environment variables
    recipe.env["VERSION"] = recipe.version

    if recipe.version_type is not None:
        recipe.env["VERSION_TYPE"] = recipe.version_type.value

    # Set branch from parameter if provided
    if branch:
        recipe.branch = branch
        logger.info(f"Set branch from parameter: {branch}")

    # Set build_name from parameter, or BUILD_NAME environment variable, or recipe
    if build_name:
        recipe.build_name = build_name
        logger.info(f"Set build_name from parameter: {build_name}")
    elif recipe.build_name is None:
        recipe.build_name = BUILD_NAME
        logger.info(
            f"Set build_name from BUILD_NAME environment variable: {BUILD_NAME}"
        )
    else:
        logger.info(f"Using build_name from recipe: {recipe.build_name}")

    # Determine previous version
    if "VERSION_PREV" not in recipe.env:
        try:
            previous_recipe = utils.get_previous_version(
                product=recipe.product, version=recipe.version
            )
            logger.info(
                f"Previous version of {recipe.product}: {previous_recipe.label} ({previous_recipe})"
            )
            recipe.env["VERSION_PREV"] = previous_recipe.label
        except (
            LookupError,
            ValueError,
            TypeError,
        ) as e:  # versions not found, or don't parse correctly
            logger.error(f"Error: {e}")

    # Add additional environment variables if provided
    if env_vars:
        recipe.env.update(env_vars)
        logger.info(f"Added env_vars to recipe.env: {env_vars}")

    # Add env vars to environ so they can be accessed during planning
    logger.info(f"Export envars: {recipe.env}")
    os.environ.update(recipe.env)

    # merge in base recipe inputs
    base_recipe = (
        recipe_from_yaml(recipe_path.parent / recipe.base_recipe, vars=vars)
        if recipe.base_recipe is not None
        else None
    )

    input_dataset_names = {d.id for d in recipe.inputs.datasets}
    if base_recipe is not None:
        for base_ds in base_recipe.inputs.datasets:
            if base_ds.id not in input_dataset_names:
                recipe.inputs.datasets.append(base_ds)

    # Fill in omitted versions
    previous_versions = {}
    if (
        recipe.inputs.missing_versions_strategy
        == RecipeInputsVersionStrategy.copy_latest_release
    ):
        previous_versions = utils.get_source_data_versions(
            PublishKey(recipe.product, "latest")
        ).to_dict()["version"]

    for ds in recipe.inputs.datasets:
        assert ds.source, f"Cannot import a dataset without a resolved source: {ds}"
        connector = connectors[ds.source]

        if ds.version is None:
            if ds.version_env_var is not None:
                # Use provided vars if available, otherwise fall back to environment
                version = (
                    vars.get(ds.version_env_var)
                    if vars is not None
                    else os.getenv(ds.version_env_var)
                )
                if version is None:
                    var_source = (
                        "provided vars" if vars is not None else "environment variables"
                    )
                    raise Exception(
                        f"Dataset {ds.id} requires version var '{ds.version_env_var}' in {var_source}"
                    )
                ds.version = version
            elif (
                recipe.inputs.missing_versions_strategy
                == RecipeInputsVersionStrategy.copy_latest_release
            ):
                ds.version = previous_versions[ds.id]
            else:
                ds.version = "latest"

        if ds.version == "latest":
            logger.info(f"Querying versions for {connector.conn_type}")
            ds.version = connector.get_latest_version(ds.id)

        # Determine edm recipes dataset details
        if ds.source == "edm.recipes.datasets":
            # Determine the file type
            # Hack for now, to accomodate existing file types
            ds.file_type = ds.file_type or recipes.get_preferred_file_type(
                ds.dataset, RECIPE_FILE_TYPE_PREFERENCE
            )
            # Determine the dataset name
            assert hasattr(connector, "get_name"), (
                f"Cannot get dataset name from connector type '{connector.conn_type}'"
            )
            ds.name = connector.get_name(ds.id, ds.version)  # type: ignore
            archive_dt = recipes.get_archive_date(ds.dataset)
            ds.archive_date = archive_dt.date() if archive_dt else None
            ds.url = recipes.get_url(ds.dataset)

    # Resolve any unresolved conf values (e.g. from the environment or provided vars)
    for conf in recipe.get_unresolved_stage_config_values():
        if "env" in conf.value_from:
            env_var = conf.value_from["env"]
            # Use provided vars if available, otherwise fall back to environment
            if vars is not None:
                if env_var not in vars:
                    raise Exception(f"build.plan requires missing var: {env_var}")
                conf.value = vars[env_var]
            else:
                if env_var not in os.environ:
                    raise Exception(f"build.plan requires missing env var: {env_var}")
                conf.value = os.environ[env_var]

    return recipe


def _apply_recipe_defaults(recipe: Recipe):
    recipe.inputs.dataset_defaults = (
        recipe.inputs.dataset_defaults or InputDatasetDefaults()
    )
    for ds in recipe.inputs.datasets:
        ds.preprocessor = ds.preprocessor or recipe.inputs.dataset_defaults.preprocessor
        ds.file_type = ds.file_type or recipe.inputs.dataset_defaults.file_type
        ds.destination = ds.destination or recipe.inputs.dataset_defaults.destination
        ds.source = ds.source or recipe.inputs.dataset_defaults.source
        ds.load_engine = ds.load_engine or recipe.inputs.dataset_defaults.load_engine


def recipe_from_yaml(
    path: Path, render_templates: bool = True, vars: dict[str, str] | None = None
) -> Recipe:
    """Import a recipe file from yaml, and validate schema.

    Supports Jinja2 template variables that are rendered from environment variables
    or directly provided vars.
    For security, only environment variables with the BUILD_ENV_* or BUILD_ENGINE_*
    prefixes are exposed to templates to prevent accidental exposure of secrets.
    Non-templated recipes are processed normally without errors.

    Args:
        path: Path to the recipe.yml file
        render_templates: If True, render Jinja2 templates from vars or BUILD_ENV_*/BUILD_ENGINE_* vars.
                         If False, preserve templates as strings (for DAG generation).
        vars: Optional dict of template variables. If provided, these are used instead
              of BUILD_ENV_*/BUILD_ENGINE_* environment variables. All vars or none approach.

    Returns:
        Recipe object with schema validated
    """
    with open(path, "r", encoding="utf-8") as f:
        raw_content = f.read()

    # Render Jinja2 templates
    rendered_content = raw_content
    if render_templates:
        # Use provided vars if available, otherwise fall back to BUILD_ENV_* and BUILD_ENGINE_* from environment
        if vars is not None:
            build_env_vars = vars
        else:
            # Filter environment variables to only BUILD_ENV_* and BUILD_ENGINE_* for security
            # This prevents secrets from being accidentally exposed in recipes
            build_env_vars = {
                key: value
                for key, value in os.environ.items()
                if key.startswith("BUILD_ENV_") or key.startswith("BUILD_ENGINE_")
            }

        # Use StrictUndefined to catch missing variables
        try:
            jinja_env = Environment(undefined=StrictUndefined)
            template = jinja_env.from_string(raw_content)
            rendered_content = template.render(**build_env_vars)
        except UndefinedError as e:
            # Missing template variable - provide clear error
            error_source = (
                "provided vars"
                if vars is not None
                else "BUILD_ENV_* or BUILD_ENGINE_* environment variables"
            )
            raise ValueError(
                f"Recipe template requires variable that is not set in {error_source}: {e}."
            ) from e
        except TemplateSyntaxError as e:
            # Invalid Jinja2 syntax - provide clear error
            raise ValueError(
                f"Recipe contains invalid Jinja2 template syntax at line {e.lineno}: {e.message}"
            ) from e
    else:
        # Preserve templates as strings for DAG generation
        # Use PreserveUndefined to keep {{ VAR }} syntax intact
        try:
            jinja_env = Environment(undefined=PreserveUndefined)
            template = jinja_env.from_string(raw_content)
            rendered_content = template.render()  # No variables provided
        except TemplateSyntaxError as e:
            # Invalid Jinja2 syntax - provide clear error
            raise ValueError(
                f"Recipe contains invalid Jinja2 template syntax at line {e.lineno}: {e.message}"
            ) from e

    # Parse the rendered YAML
    s = yaml.safe_load(rendered_content)
    recipe = Recipe(**s)
    _apply_recipe_defaults(recipe)
    return recipe


def get_recipe(
    product_path: Path,
    recipe_name: str | None = None,
    render_templates: bool = True,
    vars: dict[str, str] | None = None,
) -> Recipe:
    """Load a recipe file as a pydantic Recipe model.

    Args:
        product_path: Path to the product directory
        recipe_name: Name of the recipe file (e.g., "my-recipe").
                    If None, defaults to "recipe"
        render_templates: If True, render Jinja2 templates from vars or BUILD_ENV_* vars.
                         If False, preserve templates as strings (for DAG generation).
        vars: Optional dict of template variables. If provided, these are used instead
              of BUILD_ENV_* environment variables.

    Returns:
        Recipe pydantic model
    """
    recipe_path = config.get_recipe_path(product_path, recipe_name)
    return recipe_from_yaml(recipe_path, render_templates=render_templates, vars=vars)


def get_recipe_lock(
    product_path: Path,
    recipe_name: str | None = None,
    render_templates: bool = True,
    vars: dict[str, str] | None = None,
) -> Recipe:
    """Load a recipe lockfile as a pydantic Recipe model.

    Args:
        product_path: Path to the product directory
        recipe_name: Name of the recipe file (e.g., "my-recipe").
                    If None, defaults to "recipe"
        render_templates: If True, render Jinja2 templates from vars or BUILD_ENV_* vars.
                         If False, preserve templates as strings (for DAG generation).
        vars: Optional dict of template variables. If provided, these are used instead
              of BUILD_ENV_* environment variables.

    Returns:
        Recipe pydantic model from the lockfile

    Priority:
        1. BUILD_ENV_OUTPUT_DIR/recipe.lock.yml (during builds)
        2. product_path/recipe.lock.yml (backward compatibility)
    """
    import os

    # Priority: BUILD_ENV_OUTPUT_DIR/recipe.lock.yml > product_path/recipe.lock.yml
    if "BUILD_ENV_OUTPUT_DIR" in os.environ:
        build_dir_recipe = Path(os.environ["BUILD_ENV_OUTPUT_DIR"]) / "recipe.lock.yml"
        if build_dir_recipe.exists():
            recipe_lock_path = build_dir_recipe
        else:
            # Fall back to product directory if not found in build directory
            recipe_lock_path = config.get_recipe_lock_path(product_path, recipe_name)
    else:
        recipe_lock_path = config.get_recipe_lock_path(product_path, recipe_name)

    return recipe_from_yaml(
        recipe_lock_path, render_templates=render_templates, vars=vars
    )


def generate_lock_file(
    recipe_file: Path, recipe: Recipe, output_path: Path | None = None
) -> Path:
    if output_path:
        # Use explicit output path if provided
        # Resolve relative paths to absolute paths
        lock_file = output_path.resolve()
        lock_file.parent.mkdir(parents=True, exist_ok=True)
    elif recipe.build_name:
        # New approach: Use plan directory
        from dcpy.lifecycle.config import get_plan_dir

        plan_dir = get_plan_dir(recipe.product, recipe.build_name)
        plan_dir.mkdir(parents=True, exist_ok=True)
        lock_file = plan_dir / "recipe.lock.yml"
    else:
        # Backward compatibility: Write to product directory
        lock_file = recipe_file.parent / f"{recipe_file.stem}.lock.yml"

    with open(lock_file, "w", encoding="utf-8") as f:
        logger.info(f"Writing recipe lockfile to {str(lock_file.absolute())}")
        yaml.dump(recipe.model_dump(mode="json"), f)
    return lock_file


def plan(
    recipe_file: Path,
    version: str | None = None,
    build_name: str | None = None,
    branch: str | None = None,
    env_vars: dict[str, str] | None = None,
    output_path: Path | None = None,
) -> Path:
    logger.info(f"Planning recipe from {recipe_file}")
    recipe = plan_recipe(
        recipe_file, version, build_name=build_name, branch=branch, env_vars=env_vars
    )
    lock_file = generate_lock_file(recipe_file, recipe, output_path=output_path)
    return lock_file


def write_source_data_versions(recipe_file: Path):
    source_data_versions_path = recipe_file.parent / "source_data_versions.csv"
    logger.info(f"Writing source data versions to {source_data_versions_path}")
    recipe = recipe_from_yaml(recipe_file)
    datasets = recipe.inputs.datasets

    unresolved_versions = [x for x in datasets if x.version == "latest"]
    if unresolved_versions:
        exception = (
            "Recipe has unresolved versions! Can't write source "
            + f"data versions {unresolved_versions}"
        )
        logger.error(exception)
        raise Exception(exception)

    header = ["schema_name", "dataset_name", "v", "file_type", "archive_date", "url"]
    with open(source_data_versions_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        writer.writerows(
            {
                "schema_name": x.id,
                "dataset_name": x.name,
                "v": x.version,
                "file_type": x.file_type,
                "archive_date": x.archive_date.isoformat() if x.archive_date else "",
                "url": x.url if x.url else "",
            }
            for x in datasets
        )


def repeat_recipe_from_source_data_versions(
    version: str, source_data_versions: pd.DataFrame, template_recipe: Recipe
) -> Recipe:
    recipe = template_recipe.model_copy()
    recipe.version = version
    version_by_source_data_name = {
        name: row["version"] for name, row in source_data_versions.iterrows()
    }
    for ds in recipe.inputs.datasets:
        if ds.id in version_by_source_data_name:
            ds.version = version_by_source_data_name[ds.id]
        else:
            raise Exception(
                "Dataset found in template recipe not found in historical source data versions, \
                cannot repeat build."
            )

    return recipe


def repeat_build(
    product_key: BuildKey | DraftKey | PublishKey,
    recipe_file: Path | None = None,
    manual_version: str | None = None,
) -> Path:
    connector = get_published_default_connector()

    # Get version string from product_key (handles BuildKey.build, DraftKey/PublishKey.version)
    if isinstance(product_key, BuildKey):
        key_version = product_key.build
    elif hasattr(product_key, "version"):
        key_version = product_key.version  # type: ignore[attr-defined]
    else:
        raise TypeError(f"Unsupported product_key type: {type(product_key)}")

    if connector.resource_exists(
        product_key.product, key_version, "build_metadata.json"
    ):
        # Get file contents and deserialize locally (breaks circular dependency)
        file_contents = utils.get_file_contents(product_key, "build_metadata.json")
        s = yaml.safe_load(file_contents)["recipe"]
        recipe = Recipe(**s)
    elif connector.resource_exists(
        product_key.product, key_version, "source_data_versions.csv"
    ):
        logger.info(
            f"Attempting to repeat recipe for {product_key} from source_data_versions.csv"
        )

        if not (recipe_file and recipe_file.exists()):
            raise ValueError(
                "Recipe file for template must be supplied in if repeating an older build without build_metadata.json"
            )

        if hasattr(product_key, "version"):
            version = product_key.version
        elif manual_version:
            version = manual_version
        else:
            raise ValueError(
                "Version must be supplied manually if repeating an older build without build_metadata.json"
            )

        template_recipe = recipe_from_yaml(recipe_file)
        source_data_versions = utils.get_source_data_versions(product_key)
        recipe = repeat_recipe_from_source_data_versions(
            version, source_data_versions, template_recipe
        )

    else:
        raise Exception(
            f"Neither 'build_metadata.json' nor 'source_data_versions.csv' can be found. '{product_key}' cannot be repeated"
        )

    recipe_file = recipe_file or Path(DEFAULT_RECIPE)
    lock_file = generate_lock_file(recipe_file, recipe)
    return lock_file


app = typer.Typer(add_completion=False)


@app.command("recipe")
def _cli_wrapper_plan_recipe(
    recipe_path: Path = typer.Option(
        Path(DEFAULT_RECIPE),
        "--recipe-path",
        "-r",
        help="Path of recipe file to use",
    ),
    version=typer.Option(
        None,
        "--version",
        "-v",
        help="Version of dataset being built",
    ),
    repeat: bool = typer.Option(
        False, "--repeat", help="Repeat specific published build"
    ),
    output_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Path to write recipe.lock.yml (default: auto-determined based on build_name)",
    ),
):
    plan(recipe_path, version, output_path=output_path)


@app.command("repeat")
def _cli_wrapper_repeat_recipe(
    product: str = typer.Argument(help="Name of the product to build"),
    product_type: str = typer.Option(
        None, "--product-type", "-t", help="Product/build type ('draft' or 'publish')"
    ),
    version_or_build: str = typer.Option(
        None,
        "--version-or-build",
        "-vb",
        help="Unique key for build/draft/publish build, either version or build name, respectively",
    ),
    draft_revision_number: int = typer.Option(
        None,
        "--draft-number",
        "-dn",
        help="If --product-type is 'draft', must provide draft revision number. Otherwise leave this blank",
    ),
    recipe_path: Path = typer.Option(
        Path(DEFAULT_RECIPE),
        "--recipe-path",
        "-r",
        help="Path of recipe file to use. Only needed if attempting to rebuild from older builds without build_metadata.json",
    ),
    manual_version: str = typer.Option(
        None,
        "--manual-version",
        "--mv",
        help="Manually specified version. Only needed if attempting to rebuild and older draft where version cannot be easily determined.",
    ),
):
    # TODO: publishing connector refactor - move get_draft_revision_label to drafts.py
    from dcpy.connectors.edm.publishing import get_draft_revision_label

    product_key: BuildKey | DraftKey | PublishKey
    product_label = (
        "db-green-fast-track" if product == "green_fast_track" else f"db-{product}"
    )
    match product_type:
        case "build":
            product_key = BuildKey(product=product_label, build=version_or_build)
        case "draft":
            if draft_revision_number is None:
                raise ValueError(
                    "For repeating builds of 'draft' type, need to provide draft revision number"
                )
            draft_revision = get_draft_revision_label(
                product=product_label,
                version=version_or_build,
                revision_num=draft_revision_number,
            )
            product_key = DraftKey(
                product=product_label,
                version=version_or_build,
                revision=draft_revision,
            )
        case "publish":
            product_key = PublishKey(product=product_label, version=version_or_build)
        case _:
            raise ValueError(
                f"Invalid product/build type supplied: '{version_or_build}'. Only options are 'build', 'draft', or 'publish'"
            )
    repeat_build(product_key, recipe_path), manual_version


if __name__ == "__main__":
    app()
